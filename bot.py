import os
import json
import asyncio
import httpx
from collections import deque
from datetime import datetime, date
import re
from dotenv import load_dotenv
from openai import AsyncOpenAI
from fastapi import FastAPI, Request, BackgroundTasks
import uvicorn

# ==========================================
# 1. CREDENCIAIS & VARIÁVEIS DE AMBIENTE
# ==========================================
load_dotenv()
OPENAI_API_KEY   = os.getenv('OPENAI_API_KEY')
ZAPI_INSTANCE_ID = os.getenv('ZAPI_INSTANCE_ID')
ZAPI_TOKEN       = os.getenv('ZAPI_TOKEN')
SUPABASE_URL     = os.getenv('SUPABASE_URL')
SUPABASE_KEY     = os.getenv('SUPABASE_KEY')
SITE_URL         = os.getenv('SITE_URL', 'app.seusite.com.br')

client_openai = AsyncOpenAI(api_key=OPENAI_API_KEY)
app = FastAPI()

# Memória do Bot (em RAM)
historico_usuarios  = {}
contador_lembretes  = {}

# ==========================================
# BUFFER DE MENSAGENS (anti-spam / debounce)
# ==========================================
BUFFER_SEGUNDOS   = 10          # tempo de espera antes de processar
buffer_mensagens  = {}          # telefone -> lista de textos acumulados
buffer_tasks      = {}          # telefone -> asyncio.Task ativo

# ==========================================
# 2. PERSONALIDADE & PROATIVIDADE
# ==========================================
PROMPTS_PERSONALIDADE = {
    "friendly": os.getenv('PROMPT_FRIENDLY', "Aja como um parceiro/brother, super informal e autêntico."),
    "direct":   os.getenv('PROMPT_DIRECT',   "Seja direto, objetivo e sem enrolação."),
    "formal":   os.getenv('PROMPT_FORMAL',   "Aja como um consultor financeiro formal e educado.")
}

LIMITES_PROATIVIDADE = {
    "low":    8,
    "medium": 4,
    "high":   2
}

# ==========================================
# 3. Z-API — ENVIO DE MENSAGENS
# ==========================================
async def enviar_mensagem_whatsapp(telefone: str, texto: str):
    url = f"https://api.z-api.io/instances/{ZAPI_INSTANCE_ID}/token/{ZAPI_TOKEN}/send-text"

    # Z-API espera o número COM código do país mas SEM o '+' — ex: 5522999998888
    # Garante que está limpo e no formato correto
    num = re.sub(r'\D', '', str(telefone).strip())
    if not num.startswith('55'):
        num = '55' + num
    telefone_str = num

    payload = {"phone": telefone_str, "message": str(texto)}
    headers = {"Content-Type": "application/json"}
    zapi_client_token = os.getenv('ZAPI_CLIENT_TOKEN')
    if zapi_client_token:
        headers["Client-Token"] = zapi_client_token

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(url, json=payload, headers=headers)
            if not response.is_success:
                print(f"❌ [ERRO ENVIO] {telefone_str} → {response.status_code}: {response.text}")
            else:
                print(f"🚀 [ENVIADO] Para {telefone_str}")
        except Exception as e:
            print(f"❌ [ERRO ENVIO] {telefone_str}: {e}")

def extrair_essencia_telefone(telefone: str) -> str:
    if not telefone: return ""
    num = re.sub(r'\D', '', str(telefone))
    if num.startswith('55') and len(num) >= 12:
        num = num[2:]
    if len(num) >= 10:
        return f"{num[:2]}{num[-8:]}"
    return num

# ==========================================
# 4. SUPABASE — HELPERS BASE
# ==========================================
def sb_headers() -> dict:
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=representation"
    }

async def sb_get(client: httpx.AsyncClient, tabela: str, filtros: str = "") -> list:
    url = f"{SUPABASE_URL}/rest/v1/{tabela}?{filtros}"
    res = await client.get(url, headers=sb_headers())
    if res.status_code == 200:
        return res.json()
    print(f"⚠️ [SUPABASE GET] {tabela} → {res.status_code}: {res.text}")
    return []

async def sb_post(client: httpx.AsyncClient, tabela: str, dados: dict) -> dict | None:
    url = f"{SUPABASE_URL}/rest/v1/{tabela}"
    res = await client.post(url, json=dados, headers=sb_headers())
    if res.status_code in (200, 201):
        resultado = res.json()
        return resultado[0] if isinstance(resultado, list) and resultado else resultado
    print(f"⚠️ [SUPABASE POST] {tabela} → {res.status_code}: {res.text}")
    return None

# ==========================================
# 5. SUPABASE — ACESSO & PERFIL
# ==========================================
async def buscar_todos_telefones_tabela(client, tabela: str, nome_coluna_id: str) -> dict:
    mapa = {}
    offset, limit = 0, 1000
    while True:
        rows = await sb_get(client, tabela, f"select=telefone,{nome_coluna_id}&limit={limit}&offset={offset}")
        if not rows: break
        for row in rows:
            tel     = str(row.get('telefone') or '').strip()
            user_id = row.get(nome_coluna_id)
            if tel and user_id:
                mapa[extrair_essencia_telefone(tel)] = user_id
        if len(rows) < limit: break
        offset += limit
    return mapa

async def buscar_config_usuario(client, user_id: str) -> dict:
    rows = await sb_get(client, "configuracoes_usuario", f"id=eq.{user_id}&select=*")
    return rows[0] if rows else {}

async def buscar_config_membro(client, telefone_limpo: str) -> tuple[dict, str | None, str | None]:
    """
    Busca membro na tabela membros_familia pelo telefone.
    Retorna (config_do_membro, user_id_do_membro, dono_id)
    O membro tem suas próprias configs (personalidade, metas, etc) dentro da própria linha.
    """
    sufixo = telefone_limpo[-8:]
    rows = await sb_get(client, "membros_familia",
        f"telefone=like.%25{sufixo}%25&select=id,dono_id,convidado_id,nome,personalidade_bot,proatividade_bot,dicas_economia,sugestoes_excedente,economia_automatica,metas,renda_mensal,faixa_renda")
    if not rows:
        return {}, None, None
    m = rows[0]
    # user_id real do membro é o convidado_id (se aceito) ou o id da linha
    user_id_membro = m.get("convidado_id") or str(m.get("id"))
    dono_id        = str(m.get("dono_id")) if m.get("dono_id") else None
    config = {
        "personalidade_bot":   m.get("personalidade_bot",  "friendly"),
        "proatividade_bot":    m.get("proatividade_bot",   "medium"),
        "dicas_economia":      m.get("dicas_economia",     True),
        "sugestoes_excedente": m.get("sugestoes_excedente",True),
        "economia_automatica": m.get("economia_automatica",False),
        "metas":               m.get("metas",              "[]"),
        "renda_mensal":        m.get("renda_mensal",       0),
        "faixa_renda":         m.get("faixa_renda",        ""),
        "_nome":               m.get("nome", ""),
        "_is_membro":          True,
        "_dono_id":            dono_id,
    }
    return config, user_id_membro, dono_id

async def verificar_admin_user(client, telefone_limpo: str) -> tuple[bool, str]:
    """
    Verifica na tabela admin_users se o número tem acesso.
    Retorna (liberado: bool, motivo_bloqueio: str)
    """
    # Busca pelo sufixo dos últimos 8 dígitos (cobre variações de DDI)
    sufixo = telefone_limpo[-8:]
    rows = await sb_get(client, "admin_users", f"celular=like.%25{sufixo}%25&select=*")

    if not rows:
        return False, "nao_cadastrado"

    user = rows[0]

    # Inativo no admin
    if not user.get("ativo", True):
        return False, "inativo"

    # Vitalício — sempre libera
    if user.get("plano_vitalicio"):
        print(f"♾️  [ADMIN] {telefone_limpo} — plano vitalício ativo")
        return True, "vitalicio"

    # Usuário de teste — verifica período
    if user.get("usuario_teste"):
        dias_teste  = int(user.get("dias_teste") or 0)
        criado_em   = user.get("criado_em", "")
        try:
            # Parse da data de criação (ISO 8601)
            criado_dt   = datetime.fromisoformat(criado_em.replace("Z", "+00:00"))
            dias_passados = (datetime.now(criado_dt.tzinfo) - criado_dt).days
            if dias_passados <= dias_teste:
                restantes = dias_teste - dias_passados
                print(f"🧪 [ADMIN] {telefone_limpo} — teste: {dias_passados}/{dias_teste} dias ({restantes} restantes)")
                return True, f"teste_{restantes}"
            else:
                print(f"⛔ [ADMIN] {telefone_limpo} — teste expirado ({dias_passados} dias, limite {dias_teste})")
                return False, "teste_expirado"
        except Exception as e:
            print(f"⚠️ [ADMIN] Erro ao calcular período de teste: {e}")
            return False, "erro_teste"

    # Cadastrado mas sem plano definido
    return False, "sem_plano"

async def verificar_acesso_e_perfil(telefone_zapi: str):
    """
    Retorna (tem_acesso: bool, config: dict, user_id: str | None)

    3 portas de entrada INDEPENDENTES — basta estar em UMA delas:

    PORTA 1 — admin_users (celular)
        Pessoas adicionadas manualmente (teste ou vitalício).
        Verifica ativo=true + prazo de teste se aplicável.

    PORTA 2 — assinaturas (telefone)
        Quem pagou de verdade. Verifica ativo=true.
        Config vem de configuracoes_usuario pelo id.

    PORTA 3 — membros_familia (telefone)
        Convidados da família. Só libera se o DONO (dono_id)
        tiver assinatura ativa em assinaturas.
        Config vem da própria linha do membro.
    """
    if not SUPABASE_URL:
        return None, None, None

    tel_limpo = extrair_essencia_telefone(telefone_zapi)
    sufixo    = tel_limpo[-8:]

    async with httpx.AsyncClient() as client:

        # ══════════════════════════════════════════
        # PORTA 1 — admin_users (controle manual)
        # ══════════════════════════════════════════
        rows_admin = await sb_get(client, "admin_users",
            f"celular=like.%25{sufixo}%25&select=*")
        if rows_admin:
            adm = rows_admin[0]
            if adm.get("ativo"):
                liberado, motivo = await verificar_admin_user(client, tel_limpo)
                if liberado:
                    # Tenta pegar config real pelo email
                    email_adm = adm.get("email", "")
                    user_id   = None
                    config    = {}
                    if email_adm:
                        ass_rows = await sb_get(client, "assinaturas",
                            f"email=eq.{email_adm}&select=id,ativo")
                        if ass_rows:
                            user_id = ass_rows[0].get("id")
                            config  = await buscar_config_usuario(client, user_id) if user_id else {}
                    config["_acesso_motivo"] = motivo
                    config["_is_membro"]     = False
                    print(f"🔑 [PORTA 1 — ADMIN] {tel_limpo} | {adm.get('nome','?')} | motivo={motivo}")
                    return True, config, user_id
                else:
                    print(f"🚫 [PORTA 1 — ADMIN] {tel_limpo} bloqueado — {motivo}")
                    return False, {"_acesso_motivo": motivo}, None

        # ══════════════════════════════════════════
        # PORTA 2 — assinaturas (pagante direto)
        # ══════════════════════════════════════════
        rows_ass = await sb_get(client, "assinaturas",
            f"telefone=like.%25{sufixo}%25&select=id,nome,ativo,plano")
        if rows_ass:
            ass = rows_ass[0]
            if not ass.get("ativo", False):
                print(f"🚫 [PORTA 2 — ASSINATURA] {tel_limpo} — assinatura inativa")
                return False, {"_acesso_motivo": "assinatura_inativa"}, None
            user_id = ass.get("id")
            config  = await buscar_config_usuario(client, user_id) if user_id else {}
            config["_acesso_motivo"] = "assinatura_ativa"
            config["_is_membro"]     = False
            print(f"💳 [PORTA 2 — ASSINATURA] {tel_limpo} | {ass.get('nome','?')} | plano={ass.get('plano','?')}")
            return True, config, user_id

        # ══════════════════════════════════════════
        # PORTA 3 — membros_familia (convidado)
        # ══════════════════════════════════════════
        rows_membro = await sb_get(client, "membros_familia",
            f"telefone=like.%25{sufixo}%25&select=*")
        if rows_membro:
            membro  = rows_membro[0]
            dono_id = membro.get("dono_id")

            # Verifica se o DONO tem assinatura ativa
            dono_ativo = False
            if dono_id:
                rows_dono = await sb_get(client, "assinaturas",
                    f"id=eq.{dono_id}&select=ativo")
                if rows_dono and rows_dono[0].get("ativo"):
                    dono_ativo = True
                # Também aceita se o dono estiver em admin_users ativo
                if not dono_ativo:
                    rows_dono_adm = await sb_get(client, "admin_users",
                        f"id=eq.{dono_id}&ativo=eq.true&select=id")
                    if rows_dono_adm:
                        dono_ativo = True

            if not dono_ativo:
                print(f"🚫 [PORTA 3 — MEMBRO] {tel_limpo} — dono sem assinatura ativa")
                return False, {"_acesso_motivo": "dono_sem_assinatura"}, None

            user_id_membro = membro.get("convidado_id") or str(membro.get("id"))
            config = {
                "personalidade_bot":   membro.get("personalidade_bot",   "friendly"),
                "proatividade_bot":    membro.get("proatividade_bot",    "medium"),
                "dicas_economia":      membro.get("dicas_economia",      True),
                "sugestoes_excedente": membro.get("sugestoes_excedente", True),
                "economia_automatica": membro.get("economia_automatica", False),
                "metas":               membro.get("metas",               "[]"),
                "renda_mensal":        membro.get("renda_mensal",        0),
                "faixa_renda":         membro.get("faixa_renda",         ""),
                "_nome":               membro.get("nome",                ""),
                "_is_membro":          True,
                "_dono_id":            str(dono_id) if dono_id else None,
                "_acesso_motivo":      "membro_familia",
            }
            print(f"👨‍👩‍👧 [PORTA 3 — MEMBRO] {tel_limpo} | {membro.get('nome','?')} | dono_id={dono_id}")
            return True, config, user_id_membro

        # Número não encontrado em nenhuma das 3 portas
        print(f"🚫 [ACESSO] {tel_limpo} não encontrado em nenhuma tabela")
        return False, {"_acesso_motivo": "nao_cadastrado"}, None

# ==========================================
# 6. SUPABASE — LEITURA DE DADOS
# ==========================================
async def buscar_transacoes(user_id: str, tipo: str = None, mes_atual: bool = True) -> list:
    """Busca transactions. tipo = 'income' | 'expense' | None (todos)"""
    async with httpx.AsyncClient() as client:
        filtros = f"user_id=eq.{user_id}&order=date.desc&limit=50"
        if tipo:
            filtros += f"&type=eq.{tipo}"
        if mes_atual:
            hoje  = date.today()
            inicio = f"{hoje.year}-{hoje.month:02d}-01T00:00:00"
            filtros += f"&date=gte.{inicio}"
        return await sb_get(client, "transactions", filtros)

async def buscar_financas_fixas(user_id: str, tipo: str = None) -> list:
    """Busca recurring_finances. tipo = 'income' | 'expense' | None"""
    async with httpx.AsyncClient() as client:
        filtros = f"user_id=eq.{user_id}&order=created_at.desc"
        if tipo:
            filtros += f"&type=eq.{tipo}"
        return await sb_get(client, "recurring_finances", filtros)

async def buscar_lembretes(user_id: str) -> list:
    """Busca lembretes futuros não completados"""
    async with httpx.AsyncClient() as client:
        hoje   = date.today().isoformat()
        filtros = f"user_id=eq.{user_id}&completed=eq.false&date=gte.{hoje}&order=date.asc&limit=10"
        return await sb_get(client, "reminders", filtros)

async def buscar_membros_familia(user_id: str) -> list:
    """Busca membros da família do dono"""
    async with httpx.AsyncClient() as client:
        filtros = f"dono_id=eq.{user_id}&select=nome,email,telefone,status,renda_mensal"
        return await sb_get(client, "membros_familia", filtros)

# ==========================================
# 7. SUPABASE — ESCRITA DE DADOS
# ==========================================
async def salvar_transacao(user_id: str, dados_ia: dict) -> bool:
    """Salva em 'transactions'"""
    valor_bruto = dados_ia.get('valor')
    try:
        valor = float(valor_bruto) if valor_bruto is not None else 0.0
    except (ValueError, TypeError):
        valor = 0.0

    tipo_br = dados_ia.get('tipo', '')
    tipo_db = 'income' if tipo_br == 'Receita' else 'expense'

    payload = {
        "user_id":     user_id,
        "description": dados_ia.get('descricao') or dados_ia.get('categoria') or 'Sem descrição',
        "amount":      valor,
        "type":        tipo_db,
        "category":    dados_ia.get('categoria') or 'Geral',
        "date":        datetime.utcnow().isoformat()
    }

    async with httpx.AsyncClient() as client:
        resultado = await sb_post(client, "transactions", payload)
        if resultado:
            print(f"✅ [TRANSACTION] Salvo: {tipo_db} R${valor:.2f}")
            return True
    return False

async def salvar_financa_fixa(user_id: str, dados_ia: dict) -> bool:
    """Salva em 'recurring_finances'"""
    valor_bruto = dados_ia.get('valor')
    try:
        valor = float(valor_bruto) if valor_bruto is not None else 0.0
    except (ValueError, TypeError):
        valor = 0.0

    tipo_br = dados_ia.get('tipo', '')
    tipo_db = 'income' if tipo_br == 'Receita' else 'expense'

    # Extrai o dia de vencimento se vier no campo data_recorrencia
    due_day = None
    data_rec_str = dados_ia.get('data_recorrencia') or ''
    match = re.search(r'\b(\d{1,2})\b', data_rec_str)
    if match:
        d = int(match.group(1))
        if 1 <= d <= 31:
            due_day = d

    # Frequência
    freq_map = {
        'semanal': 'Semanal', 'semana': 'Semanal',
        'quinzenal': 'Quinzenal', 'quinzena': 'Quinzenal',
        'mensal': 'Mensal', 'mês': 'Mensal', 'mes': 'Mensal',
        'anual': 'Anual', 'ano': 'Anual'
    }
    freq_raw  = (data_rec_str or '').lower()
    frequency = 'Mensal'
    for k, v in freq_map.items():
        if k in freq_raw:
            frequency = v
            break

    payload = {
        "user_id":     user_id,
        "description": dados_ia.get('descricao') or dados_ia.get('categoria') or 'Sem descrição',
        "amount":      valor,
        "type":        tipo_db,
        "frequency":   frequency,
        "due_day":     due_day
    }

    async with httpx.AsyncClient() as client:
        resultado = await sb_post(client, "recurring_finances", payload)
        if resultado:
            print(f"✅ [RECURRING] Salvo: {tipo_db} R${valor:.2f} ({frequency})")
            return True
    return False

async def salvar_lembrete(user_id: str, dados_ia: dict) -> bool:
    """Salva em 'reminders'"""
    # Tenta parsear a data
    data_rec_str = dados_ia.get('data_recorrencia') or ''
    data_lembrete = date.today().isoformat()

    # Tenta extrair data no formato DD/MM/YYYY ou DD/MM
    match_data = re.search(r'(\d{1,2})[/\-](\d{1,2})(?:[/\-](\d{2,4}))?', data_rec_str)
    if match_data:
        dia = int(match_data.group(1))
        mes = int(match_data.group(2))
        ano = int(match_data.group(3)) if match_data.group(3) else date.today().year
        if ano < 100:
            ano += 2000
        try:
            data_lembrete = date(ano, mes, dia).isoformat()
        except ValueError:
            pass

    # Hora (se vier) — cobre: "10h", "10:30", "às 10h", "10h30", "10 horas", "10:00"
    hora_lembrete = None
    # Busca hora tanto no data_recorrencia quanto na descricao (a IA pode colocar em qualquer um)
    texto_hora = (data_rec_str + ' ' + (dados_ia.get('descricao') or '')).lower()
    match_hora = re.search(
        r'(?:às?\s*|as\s*)?(\d{1,2})\s*[h:]\s*(\d{2})|(?:às?\s*|as\s*)(\d{1,2})\s*h(?:oras?)?(?!\d)',
        texto_hora
    )
    if match_hora:
        if match_hora.group(1):
            # formato "10h30" ou "10:30"
            hora_lembrete = f"{int(match_hora.group(1)):02d}:{match_hora.group(2)}:00"
        elif match_hora.group(3):
            # formato "às 10h" sem minutos → :00
            hora_lembrete = f"{int(match_hora.group(3)):02d}:00:00"

    # Frequência
    freq_map = {
        'todo dia': 'Diário', 'diário': 'Diário', 'diario': 'Diário',
        'toda semana': 'Semanal', 'semanal': 'Semanal',
        'todo mês': 'Mensal', 'mensal': 'Mensal', 'mensalmente': 'Mensal'
    }
    freq_raw  = (data_rec_str + ' ' + dados_ia.get('descricao', '')).lower()
    frequency = 'Único'
    for k, v in freq_map.items():
        if k in freq_raw:
            frequency = v
            break

    payload = {
        "user_id":   user_id,
        "title":     dados_ia.get('descricao') or 'Lembrete',
        "date":      data_lembrete,
        "time":      hora_lembrete,
        "completed": False,
        "frequency": frequency
    }

    async with httpx.AsyncClient() as client:
        resultado = await sb_post(client, "reminders", payload)
        if resultado:
            print(f"✅ [REMINDER] Salvo: {payload['title']} em {data_lembrete}")
            return True
    return False

# ==========================================
# 8. CONSULTAS INTELIGENTES (RESPOSTAS RICAS)
# ==========================================
def formatar_moeda(valor) -> str:
    try:
        return f"R$ {float(valor):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
    except:
        return "R$ 0,00"

RODAPE_ZAPPOUPE = f"\n\n_💡 Gerencie tudo com mais detalhes em *{SITE_URL}*_"

async def responder_consulta(inten: str, user_id: str, personalidade: str) -> str:
    mes_nome = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"][date.today().month - 1]
    ano      = date.today().year

    # ── GASTOS DO MÊS ──────────────────────────────────────────
    if inten == 'Consulta_Gastos':
        rows = await buscar_transacoes(user_id, tipo='expense', mes_atual=True)
        if not rows:
            return f"📭 Nenhuma despesa em {mes_nome}/{ano} ainda!" + RODAPE_ZAPPOUPE
        total  = sum(float(r.get('amount', 0)) for r in rows)
        linhas = [f"💸 *Despesas — {mes_nome}/{ano}*", ""]
        for r in rows[:10]:
            linhas.append(f"• {r.get('description','?')}")
            linhas.append(f"  ↳ {formatar_moeda(r.get('amount',0))}")
        if len(rows) > 10:
            linhas.append(f"_... e mais {len(rows)-10} registro(s)_")
        linhas.append(f"\n━━━━━━━━━━━━━━━━")
        linhas.append(f"💰 *Total: {formatar_moeda(total)}*")
        return "\n".join(linhas) + RODAPE_ZAPPOUPE

    # ── RECEITAS DO MÊS ────────────────────────────────────────
    if inten == 'Consulta_Receitas':
        rows = await buscar_transacoes(user_id, tipo='income', mes_atual=True)
        if not rows:
            return f"📭 Nenhuma receita em {mes_nome}/{ano} ainda!" + RODAPE_ZAPPOUPE
        total  = sum(float(r.get('amount', 0)) for r in rows)
        linhas = [f"💰 *Receitas — {mes_nome}/{ano}*", ""]
        for r in rows[:10]:
            linhas.append(f"• {r.get('description','?')}")
            linhas.append(f"  ↳ {formatar_moeda(r.get('amount',0))}")
        if len(rows) > 10:
            linhas.append(f"_... e mais {len(rows)-10} registro(s)_")
        linhas.append(f"\n━━━━━━━━━━━━━━━━")
        linhas.append(f"💰 *Total: {formatar_moeda(total)}*")
        return "\n".join(linhas) + RODAPE_ZAPPOUPE

    # ── SALDO DO MÊS ───────────────────────────────────────────
    if inten == 'Consulta_Saldo':
        receitas = await buscar_transacoes(user_id, tipo='income',  mes_atual=True)
        despesas = await buscar_transacoes(user_id, tipo='expense', mes_atual=True)
        total_r  = sum(float(r.get('amount', 0)) for r in receitas)
        total_d  = sum(float(r.get('amount', 0)) for r in despesas)
        saldo    = total_r - total_d
        emoji_saldo = "🟢" if saldo >= 0 else "🔴"
        linhas = [
            f"📊 *Resumo — {mes_nome}/{ano}*",
            "",
            f"💰 Receitas:   *{formatar_moeda(total_r)}*",
            f"💸 Despesas:  *{formatar_moeda(total_d)}*",
            f"━━━━━━━━━━━━━━━━",
            f"{emoji_saldo} Saldo:      *{formatar_moeda(saldo)}*",
        ]
        return "\n".join(linhas) + RODAPE_ZAPPOUPE

    # ── RECEITAS FIXAS ─────────────────────────────────────────
    if inten == 'Consulta_Fixas_Receita':
        rows = await buscar_financas_fixas(user_id, tipo='income')
        if not rows:
            return "📭 Nenhuma receita fixa cadastrada ainda!" + RODAPE_ZAPPOUPE
        total  = sum(float(r.get('amount', 0)) for r in rows)
        linhas = ["🔄💰 *Receitas Fixas*", ""]
        for r in rows:
            dia = f" · dia {r['due_day']}" if r.get('due_day') else ""
            linhas.append(f"• {r.get('description','?')}")
            linhas.append(f"  ↳ {formatar_moeda(r.get('amount',0))} | {r.get('frequency','Mensal')}{dia}")
        linhas.append(f"\n━━━━━━━━━━━━━━━━")
        linhas.append(f"💰 *Total mensal: {formatar_moeda(total)}*")
        return "\n".join(linhas) + RODAPE_ZAPPOUPE

    # ── DESPESAS FIXAS ─────────────────────────────────────────
    if inten == 'Consulta_Fixas_Despesa':
        rows = await buscar_financas_fixas(user_id, tipo='expense')
        if not rows:
            return "📭 Nenhuma despesa fixa cadastrada ainda!" + RODAPE_ZAPPOUPE
        total  = sum(float(r.get('amount', 0)) for r in rows)
        linhas = ["🔄💸 *Despesas Fixas*", ""]
        for r in rows:
            dia = f" · dia {r['due_day']}" if r.get('due_day') else ""
            linhas.append(f"• {r.get('description','?')}")
            linhas.append(f"  ↳ {formatar_moeda(r.get('amount',0))} | {r.get('frequency','Mensal')}{dia}")
        linhas.append(f"\n━━━━━━━━━━━━━━━━")
        linhas.append(f"💸 *Total mensal: {formatar_moeda(total)}*")
        return "\n".join(linhas) + RODAPE_ZAPPOUPE

    # ── TODAS AS FIXAS ─────────────────────────────────────────
    if inten == 'Consulta_Fixas':
        rows = await buscar_financas_fixas(user_id)
        if not rows:
            return "📭 Nenhuma finança fixa cadastrada ainda!" + RODAPE_ZAPPOUPE
        receitas_f = [r for r in rows if r.get('type') == 'income']
        despesas_f = [r for r in rows if r.get('type') == 'expense']
        total_r    = sum(float(r.get('amount', 0)) for r in receitas_f)
        total_d    = sum(float(r.get('amount', 0)) for r in despesas_f)
        linhas     = ["🔄 *Finanças Fixas*", ""]
        if receitas_f:
            linhas.append("💰 *Receitas:*")
            for r in receitas_f:
                dia = f" · dia {r['due_day']}" if r.get('due_day') else ""
                linhas.append(f"  • {r.get('description','?')} — {formatar_moeda(r.get('amount',0))}{dia}")
        if despesas_f:
            linhas.append("")
            linhas.append("💸 *Despesas:*")
            for r in despesas_f:
                dia = f" · dia {r['due_day']}" if r.get('due_day') else ""
                linhas.append(f"  • {r.get('description','?')} — {formatar_moeda(r.get('amount',0))}{dia}")
        linhas.append(f"\n━━━━━━━━━━━━━━━━")
        linhas.append(f"💰 Entradas: {formatar_moeda(total_r)}  |  💸 Saídas: {formatar_moeda(total_d)}")
        return "\n".join(linhas) + RODAPE_ZAPPOUPE

    # ── LEMBRETES ──────────────────────────────────────────────
    if inten == 'Consulta_Lembretes':
        rows = await buscar_lembretes(user_id)
        if not rows:
            return "✅ Nenhum lembrete pendente por enquanto!" + RODAPE_ZAPPOUPE
        linhas = ["📅 *Próximos Lembretes*", ""]
        for r in rows:
            hora     = f" às {r['time'][:5]}" if r.get('time') else ""
            freq     = f" _{r['frequency']}_" if r.get('frequency') and r['frequency'] != 'Único' else ""
            linhas.append(f"🔔 *{r.get('title','?')}*")
            linhas.append(f"  ↳ {r.get('date','?')}{hora}{freq}")
        return "\n".join(linhas) + RODAPE_ZAPPOUPE

    # ── FAMÍLIA ────────────────────────────────────────────────
    if inten == 'Consulta_Familia':
        rows = await buscar_membros_familia(user_id)
        if not rows:
            return "👨\u200d👩\u200d👧 Nenhum membro na família ainda!" + RODAPE_ZAPPOUPE
        linhas = ["👨\u200d👩\u200d👧 *Família*", ""]
        for r in rows:
            status = "✅ Ativo" if r.get('status') == 'ativo' else "⏳ Pendente"
            renda  = f"\n  ↳ Renda: {formatar_moeda(r.get('renda_mensal',0))}" if r.get('renda_mensal') else ""
            linhas.append(f"• *{r.get('nome','?')}* — {status}{renda}")
        return "\n".join(linhas) + RODAPE_ZAPPOUPE

    return "🤔 Não entendi o que você quer consultar. Pode repetir?"

# ==========================================
# 9. INTELIGÊNCIA ARTIFICIAL — NÚCLEO
# ==========================================
async def analisar_mensagem_financeira(mensagem_texto: str, telefone: str, config: dict, user_id: str) -> str:
    if telefone not in historico_usuarios:
        historico_usuarios[telefone] = deque(maxlen=10)
    if telefone not in contador_lembretes:
        contador_lembretes[telefone] = 0

    personalidade = config.get('personalidade_bot', 'friendly')
    proatividade  = config.get('proatividade_bot',  'medium')
    dicas_ativadas = config.get('dicas_economia', True)
    metas_json    = config.get('metas', '[]')

    limite_para_lembrar          = LIMITES_PROATIVIDADE.get(proatividade, 4)
    proxima_msg_hora_de_lembrar  = (contador_lembretes[telefone] + 1 >= limite_para_lembrar)

    system_prompt = f"""Você é um assistente financeiro inteligente pelo WhatsApp.
[SUA PERSONALIDADE]: {PROMPTS_PERSONALIDADE.get(personalidade, PROMPTS_PERSONALIDADE['friendly'])}

Sua tarefa é analisar a mensagem do usuário e responder OBRIGATORIAMENTE em JSON.

═══════════════════════════════════════════
TIPOS DISPONÍVEIS:
═══════════════════════════════════════════
Registros (gravam no banco):
  • 'Receita'  → entrada de dinheiro NÃO recorrente (ex: "recebi 500 de freela")
  • 'Despesa'  → gasto NÃO recorrente (ex: "gastei 50 no mercado")
  • 'Receita_Fixa'  → entrada recorrente (ex: "meu salário é 3000 todo mês", "recebo aluguel de 800 todo dia 5")
  • 'Despesa_Fixa'  → gasto recorrente (ex: "pago internet 100 todo mês", "academia 80 mensalmente")
  • 'Lembrete' → agendamento financeiro (ex: "me lembra de pagar o boleto sexta")
  • 'Meta'     → definição de meta (ex: "quero guardar 200 por mês")

Consultas (leem o banco e respondem):
  • 'Consulta_Gastos'         → "quanto gastei?", "minhas despesas do mês"
  • 'Consulta_Receitas'       → "quanto recebi?", "minhas receitas"
  • 'Consulta_Saldo'          → "qual meu saldo?", "como tá minha situação financeira?"
  • 'Consulta_Fixas'          → "quais são minhas contas fixas?", "minhas fixas"
  • 'Consulta_Fixas_Receita'  → "minhas receitas fixas", "o que recebo todo mês?"
  • 'Consulta_Fixas_Despesa'  → "minhas despesas fixas", "o que pago todo mês?"
  • 'Consulta_Lembretes'      → "meus lembretes", "o que tenho pra pagar?"
  • 'Consulta_Familia'        → "quem tá na minha família?", "membros da família"

Remoção:
  • 'Remocao' → quando o usuário pedir pra apagar, remover, excluir, cancelar ou editar QUALQUER coisa
    (ex: "remove esse lembrete", "apaga a despesa", "cancela a conta fixa", "quero deletar isso")

Conversa:
  • 'Conversa' → mensagens que NÃO têm relação com finanças (saudações, bate-papo, dúvidas sobre o bot)

ATENÇÃO: Classifique como 'Receita_Fixa' ou 'Despesa_Fixa' quando o usuário mencionar recorrência
(todo mês, mensalmente, todo dia X, semanal, quinzenal, anual, etc).
NUNCA force mensagem casual em tipo financeiro.
═══════════════════════════════════════════
"""

    # ── Dicas de economia ─────────────────────────────────────────
    if dicas_ativadas:
        system_prompt += "\nDê dicas de economia APENAS se o tipo for 'Receita', 'Despesa', 'Receita_Fixa' ou 'Despesa_Fixa'.\n"
    else:
        system_prompt += "\nNÃO dê dicas de economia. O usuário desativou essa função.\n"

    # ── Proatividade + Metas ───────────────────────────────────
    # Só ativa o gancho de metas se dicas_economia estiver ON
    # (dicas desligadas = sem proatividade, sem lembretes de meta)
    if dicas_ativadas and proxima_msg_hora_de_lembrar and metas_json and metas_json != '[]':
        system_prompt += f"""
[METAS ATIVAS DO USUÁRIO]: {metas_json}
SE classificar como 'Receita' ou 'Despesa', faça um gancho motivacional curto com a meta mais relevante.
Para qualquer outro tipo, IGNORE as metas completamente.
"""
    else:
        system_prompt += "\nNÃO mencione metas financeiras nesta resposta.\n"

    system_prompt += """
O JSON deve conter EXATAMENTE estas chaves:
- 'tipo'             : string (um dos tipos listados acima, ou 'Pendente' se faltar info)
- 'fixa'             : boolean
- 'valor'            : number ou null
- 'categoria'        : string ou null
- 'data_recorrencia' : string ou null (ex: "todo dia 5", "toda sexta", "20/06/2025 às 10h")
- 'descricao'        : string — crie um título limpo e inteligente, NUNCA repita literalmente o que o usuário disse. Ex: "reunião toda terça às 9h" → "Reunião Semanal de Trabalho" | "pago internet 100 todo mês" → "Plano de Internet" | "academia 80 mensalmente" → "Mensalidade Academia" | "recebi salário" → "Salário Mensal"
- 'pendente'         : boolean — true se faltam informações ESSENCIAIS para registrar. Caso contrário false.
- 'pergunta'         : string ou null — SE pendente=true, escreva UMA pergunta curta e direta para obter a info que falta. Exemplos:
    • Tipo desconhecido (ex: "500 de lanche"): "Foi uma despesa ou uma receita?"
    • Lembrete sem hora (ex: "lembrete todo dia"): "Que horas devo te lembrar?"
    • Lembrete sem data (ex: "lembrete às 9h"): "Qual dia ou com qual frequência?"
    • Lembrete sem nada além da hora: "Qual dia ou frequência desse lembrete?"
    • Finança fixa sem valor: "Qual o valor?"
    Use a personalidade definida na pergunta. Se não falta nada, coloque null.
- 'resposta_amigavel': string — SEMPRE preencha com a resposta ao usuário. Se pendente=true, use apenas a pergunta do campo 'pergunta' como resposta. Nunca deixe vazio.
"""

    mensagens_api = [{"role": "system", "content": system_prompt}]
    mensagens_api.extend(historico_usuarios[telefone])
    mensagens_api.append({"role": "user", "content": mensagem_texto})

    response = await client_openai.chat.completions.create(
        model="gpt-3.5-turbo",
        response_format={"type": "json_object"},
        messages=mensagens_api,
        temperature=0.7
    )

    resposta_texto = response.choices[0].message.content.strip()
    dados          = json.loads(resposta_texto)

    historico_usuarios[telefone].append({"role": "user",      "content": mensagem_texto})
    historico_usuarios[telefone].append({"role": "assistant", "content": resposta_texto})

    tipo_str     = dados.get('tipo', 'Desconhecido')
    resposta_bot = dados.get('resposta_amigavel', 'Entendido!')

    # ──────────────────────────────────────────────
    # PENDENTE — faltam informações, faz a pergunta
    # ──────────────────────────────────────────────
    if dados.get('pendente') or tipo_str == 'Pendente':
        pergunta = dados.get('pergunta') or 'Pode me dar mais detalhes?'
        print(f"❓ [PENDENTE] Perguntando para {telefone}: {pergunta}")
        return pergunta

    # ──────────────────────────────────────────────
    # CONVERSA
    # ──────────────────────────────────────────────
    if tipo_str == 'Conversa':
        print(f"💬 [CONVERSA] {telefone}")
        return resposta_bot

    # ──────────────────────────────────────────────
    # REMOÇÃO — redireciona pro site
    # ──────────────────────────────────────────────
    if tipo_str == 'Remocao':
        print(f"🗑️ [REMOCAO] Solicitação de remoção de {telefone}")
        msgs_remocao = {
            "friendly": (
                f"Opa! Por aqui eu só registro e consulto, remoção e edição são pelo site mesmo, brother! 😄\n"
                f"Acessa *{SITE_URL}* e gerencia tudo de boa por lá! 🔧"
            ),
            "direct": (
                f"Remoções e edições só pelo site: *{SITE_URL}*\n"
                f"Por aqui eu apenas registro e consulto."
            ),
            "formal": (
                f"Para remover ou editar registros, acesse o painel em *{SITE_URL}*.\n"
                f"Por este canal realizo apenas registros e consultas financeiras."
            )
        }
        return msgs_remocao.get(personalidade, msgs_remocao["friendly"])

    # ──────────────────────────────────────────────
    # CONSULTAS — lê o banco e responde
    # ──────────────────────────────────────────────
    TIPOS_CONSULTA = {
        'Consulta_Gastos', 'Consulta_Receitas', 'Consulta_Saldo',
        'Consulta_Fixas',  'Consulta_Fixas_Receita', 'Consulta_Fixas_Despesa',
        'Consulta_Lembretes', 'Consulta_Familia'
    }
    if tipo_str in TIPOS_CONSULTA:
        print(f"🔍 [CONSULTA] {tipo_str} para user {user_id}")
        return await responder_consulta(tipo_str, user_id, personalidade)

    # ──────────────────────────────────────────────
    # REGISTROS — salva no banco
    # ──────────────────────────────────────────────
    valor_bruto = dados.get('valor')
    try:
        valor = float(valor_bruto) if valor_bruto is not None else 0.0
    except (ValueError, TypeError):
        valor = 0.0

    categoria  = dados.get('categoria')  or 'Geral'
    data_rec   = dados.get('data_recorrencia') or 'Não especificada'
    descricao  = dados.get('descricao', '')
    is_fixa_label = "🔄 **(FIXA)**" if dados.get('fixa') or tipo_str in ['Receita_Fixa', 'Despesa_Fixa'] else ""

    salvo = False

    if tipo_str in ['Receita', 'Despesa']:
        salvo = await salvar_transacao(user_id, dados)
        # Contador de proatividade
        contador_lembretes[telefone] += 1
        if contador_lembretes[telefone] >= limite_para_lembrar:
            contador_lembretes[telefone] = 0
            print(f"⏰ [CONTADOR] Proativo ativado! Reset.")
        else:
            print(f"🤫 [CONTADOR] {contador_lembretes[telefone]}/{limite_para_lembrar}")

    elif tipo_str in ['Receita_Fixa', 'Despesa_Fixa']:
        salvo = await salvar_financa_fixa(user_id, dados)
        contador_lembretes[telefone] += 1
        if contador_lembretes[telefone] >= limite_para_lembrar:
            contador_lembretes[telefone] = 0

    elif tipo_str == 'Lembrete':
        salvo = await salvar_lembrete(user_id, dados)
        print(f"⏸️ [CONTADOR] Lembrete — contador pausado em {contador_lembretes[telefone]}/{limite_para_lembrar}")
        # Lembrete: só a mensagem amigável, sem card de debug
        return resposta_bot

    elif tipo_str == 'Meta':
        print(f"⏸️ [CONTADOR] Meta — contador pausado.")
        # Meta: só a mensagem amigável, sem card de debug
        return resposta_bot

    # ── Card financeiro (Receita, Despesa, Receita_Fixa, Despesa_Fixa) ──
    icone_salvo = "✅" if salvo else "⚠️"

    sufixo_data = f" · {data_rec}" if data_rec != 'Não especificada' else ''
    categoria_linha = f"_{categoria}{sufixo_data}_\n\n" if categoria and categoria != 'Geral' else ''

    return f"{categoria_linha}{resposta_bot}"

# ==========================================
# 10. FLUXO PRINCIPAL COM BUFFER (DEBOUNCE)
# ==========================================
async def _executar_apos_buffer(telefone: str):
    """
    Aguarda BUFFER_SEGUNDOS. Se não chegarem novas msgs no período,
    junta tudo que ficou no buffer, processa de uma vez e responde.
    """
    await asyncio.sleep(BUFFER_SEGUNDOS)

    # Pega e limpa o buffer atomicamente
    mensagens = buffer_mensagens.pop(telefone, [])
    buffer_tasks.pop(telefone, None)

    if not mensagens:
        return

    # Junta em uma mensagem só separada por newline
    texto_completo = "\n".join(mensagens)
    qtd = len(mensagens)
    print(f"📦 [BUFFER] {telefone} — {qtd} msg(s) agrupada(s): {texto_completo[:80]!r}")

    try:
        tem_acesso, config_perfil, user_id = await verificar_acesso_e_perfil(telefone)

        if not tem_acesso:
            motivo = (config_perfil or {}).get("_acesso_motivo", "") if config_perfil else ""
            print(f"🚫 Acesso negado: {telefone} — {motivo}")

            msgs_bloqueio = {
                "teste_expirado": (
                    "⏰ Seu período de teste do *ZapPoupe* chegou ao fim!\n"
                    "Para continuar usando, acesse *zappoupe.com.br* e escolha seu plano. 🚀"
                ),
                "assinatura_inativa": (
                    "😕 Sua assinatura do *ZapPoupe* está inativa.\n"
                    "Renove em *zappoupe.com.br* para voltar a usar! 💳"
                ),
                "dono_sem_assinatura": (
                    "😕 O titular da sua conta familiar não tem uma assinatura ativa no *ZapPoupe*.\n"
                    "Peça para ele renovar em *zappoupe.com.br*."
                ),
                "nao_cadastrado": (
                    "👋 Olá! Você ainda não tem acesso ao *ZapPoupe*.\n"
                    "Acesse *zappoupe.com.br* para começar! 😊"
                ),
            }
            msg_bloqueio = msgs_bloqueio.get(motivo)
            if msg_bloqueio:
                await enviar_mensagem_whatsapp(telefone, msg_bloqueio)
            # inativo, sem_plano, erro_teste → silencioso
            return

        # Aviso de teste acabando (últimos 2 dias)
        motivo = (config_perfil or {}).get("_acesso_motivo", "")
        if motivo.startswith("teste_") and motivo != "teste_expirado":
            try:
                restantes = int(motivo.split("_")[1])
                if restantes <= 2:
                    await enviar_mensagem_whatsapp(telefone,
                        f"⚠️ Seu teste do *ZapPoupe* expira em *{restantes} dia(s)*!\n"
                        "Acesse *zappoupe.com.br* para não perder o acesso. 😉"
                    )
            except:
                pass

        msg_resposta = await analisar_mensagem_financeira(texto_completo, telefone, config_perfil, user_id)
        if not msg_resposta or not str(msg_resposta).strip():
            print(f"⚠️ [AVISO] Resposta vazia para {telefone}, abortando envio.")
            return
        await enviar_mensagem_whatsapp(telefone, msg_resposta)

    except Exception as e:
        import traceback
        print(f"❌ Erro no processamento: {e}")
        traceback.print_exc()


def agendar_buffer(telefone: str, mensagem: str):
    """
    Adiciona a mensagem ao buffer do usuário.
    Se já havia uma task rodando, cancela e reinicia o timer (debounce).
    """
    # Acumula a mensagem
    if telefone not in buffer_mensagens:
        buffer_mensagens[telefone] = []
    buffer_mensagens[telefone].append(mensagem)

    # Cancela task anterior se existir
    task_anterior = buffer_tasks.get(telefone)
    if task_anterior and not task_anterior.done():
        task_anterior.cancel()
        print(f"🔄 [BUFFER] Timer resetado para {telefone} (total: {len(buffer_mensagens[telefone])} msg(s))")

    # Cria nova task com timer zerado
    nova_task = asyncio.create_task(_executar_apos_buffer(telefone))
    buffer_tasks[telefone] = nova_task


# ==========================================
# SUGESTÃO DE EXCEDENTES — chamada pelo pg_cron
# Roda dia 1-5 de cada mês às 09:00
# ==========================================
async def gerar_sugestao_excedentes(user_id: str, telefone: str, config: dict):
    """
    Busca receitas e despesas do mês anterior e gera uma sugestão
    personalizada de como usar o excedente.
    """
    from datetime import date
    hoje  = date.today()
    # Mês anterior
    if hoje.month == 1:
        mes_ant, ano_ant = 12, hoje.year - 1
    else:
        mes_ant, ano_ant = hoje.month - 1, hoje.year

    inicio = f"{ano_ant}-{mes_ant:02d}-01T00:00:00"
    fim    = f"{hoje.year}-{hoje.month:02d}-01T00:00:00"

    async with httpx.AsyncClient() as client:
        filtros_r = f"user_id=eq.{user_id}&type=eq.income&date=gte.{inicio}&date=lt.{fim}"
        filtros_d = f"user_id=eq.{user_id}&type=eq.expense&date=gte.{inicio}&date=lt.{fim}"
        receitas  = await sb_get(client, "transactions", filtros_r)
        despesas  = await sb_get(client, "transactions", filtros_d)

    total_r   = sum(float(r.get("amount", 0)) for r in receitas)
    total_d   = sum(float(r.get("amount", 0)) for r in despesas)
    excedente = total_r - total_d

    if excedente <= 0:
        print(f"ℹ️  [EXCEDENTE] {telefone} — sem excedente no mês anterior, pulando sugestão.")
        return

    personalidade = config.get("personalidade_bot", "friendly")
    metas_json    = config.get("metas", "[]")
    nome          = config.get("_nome", "")
    cumprimento   = f"Oi {nome}! 👋\n\n" if nome else "👋\n\n"

    # Monta prompt pra sugestão inteligente
    prompt_sugestao = f"""Você é um assistente financeiro pelo WhatsApp.
Personalidade: {PROMPTS_PERSONALIDADE.get(personalidade, PROMPTS_PERSONALIDADE['friendly'])}

O usuário teve um excedente de R$ {excedente:.2f} no mês anterior
(Receitas: R$ {total_r:.2f} | Despesas: R$ {total_d:.2f}).

Metas do usuário: {metas_json}

Crie uma mensagem curta (máx 5 linhas) de sugestão de como usar esse excedente de forma inteligente.
Mencione as metas se existirem. Use emojis com moderação. Seja direto e motivador.
Termine sempre dizendo que pode gerenciar melhor pelo ZapPoupe em zappoupe.com.br."""

    try:
        response = await client_openai.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt_sugestao}],
            max_tokens=300,
            temperature=0.8
        )
        sugestao = response.choices[0].message.content.strip()
        msg_final = f"{cumprimento}📊 *Resumo do mês anterior:*\n💰 Receitas: {formatar_moeda(total_r)}\n💸 Despesas: {formatar_moeda(total_d)}\n✨ Excedente: *{formatar_moeda(excedente)}*\n\n{sugestao}"
        await enviar_mensagem_whatsapp(telefone, msg_final)
        print(f"✅ [EXCEDENTE] Sugestão enviada para {telefone}")
    except Exception as e:
        print(f"❌ [EXCEDENTE] Erro ao gerar sugestão para {telefone}: {e}")


@app.post("/cron/excedentes")
async def webhook_excedentes(request: Request, background_tasks: BackgroundTasks):
    """
    Chamado pelo pg_cron do Supabase todo dia 1-5 às 09:00.
    Dispara sugestões apenas para usuários com sugestoes_excedente=true.
    Protegido por CRON_SECRET no header Authorization.
    """
    # Segurança básica — valida o secret
    cron_secret = os.getenv("CRON_SECRET", "")
    auth_header = request.headers.get("Authorization", "")
    if cron_secret and auth_header != f"Bearer {cron_secret}":
        print("🚫 [CRON] Requisição não autorizada!")
        return {"status": "unauthorized"}

    hoje = date.today()
    if not (1 <= hoje.day <= 5):
        print(f"ℹ️  [CRON] Fora do período (dia {hoje.day}), ignorando.")
        return {"status": "fora_do_periodo"}

    print(f"🔔 [CRON] Rodando sugestão de excedentes — {hoje.isoformat()}")

    async with httpx.AsyncClient() as client:
        # Busca todos usuários com sugestoes_excedente=true
        rows_config = await sb_get(client, "configuracoes_usuario", "sugestoes_excedente=eq.true&select=id")
        # Também busca membros da família com sugestoes_excedente=true
        rows_membros = await sb_get(client, "membros_familia",
            "sugestoes_excedente=eq.true&select=convidado_id,dono_id,telefone,nome,personalidade_bot,metas")

    # Processa donos
    for row in rows_config:
        uid = row.get("id")
        if not uid:
            continue
        # Busca telefone na tabela assinaturas
        async with httpx.AsyncClient() as client:
            tel_rows = await sb_get(client, "assinaturas", f"id=eq.{uid}&select=telefone")
            config   = await buscar_config_usuario(client, uid)
        if tel_rows and tel_rows[0].get("telefone"):
            tel = tel_rows[0]["telefone"]
            background_tasks.add_task(gerar_sugestao_excedentes, uid, tel, config)

    # Processa membros
    for m in rows_membros:
        tel     = m.get("telefone")
        user_id = m.get("convidado_id") or m.get("dono_id")
        if not tel or not user_id:
            continue
        config_m = {
            "personalidade_bot": m.get("personalidade_bot", "friendly"),
            "metas":             m.get("metas", "[]"),
            "_nome":             m.get("nome", ""),
        }
        background_tasks.add_task(gerar_sugestao_excedentes, user_id, tel, config_m)

    return {"status": "ok", "dia": hoje.day}


@app.post("/webhook")
async def zapi_webhook(request: Request):
    payload = await request.json()
    if payload.get("fromMe", False) or payload.get("isGroup", False):
        return {"status": "ignored"}

    telefone       = payload.get("phone")
    texto_mensagem = ""

    if "text" in payload and isinstance(payload["text"], dict):
        texto_mensagem = payload["text"].get("message", "")
    elif "text" in payload and isinstance(payload["text"], str):
        texto_mensagem = payload["text"]

    if not telefone or not texto_mensagem:
        return {"status": "no_text"}

    # Agenda com debounce — não usa BackgroundTasks pra poder cancelar
    agendar_buffer(telefone, texto_mensagem)
    return {"status": "buffered"}


if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8000)