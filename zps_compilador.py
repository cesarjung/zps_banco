# -*- coding: utf-8 -*-
"""
Compilador ZPS141 - GitHub Actions ready

Fluxo:
1) BANCO.xlsx (primeira aba)
   - Remove TODAS as linhas em que a coluna N (índice 13) NÃO comece com "Y"
   - Mantém cabeçalho (HEADER_ROWS_BANCO primeiras linhas)
2) Arquivo do dia (BUSCA ZPS141 - DD.MM.AAAA)
   - Localiza o arquivo MAIS RECENTE pela data no NOME (DD.MM.AAAA)
   - Lê primeira aba
   - Copia B6:AJ (linhas a partir da 6, colunas B..AJ)
3) Append no BANCO
4) Remove duplicatas no BANCO ignorando colunas A, C, D e U (0,2,3,20)
5) Atualiza BANCO.xlsx no Drive
6) Grava timestamp em zps!K2
"""

import io
import os
import json
from datetime import datetime

import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
import gspread


# ===================== CONFIGURAÇÕES =====================

# Drive
FOLDER_ID = "177E69Fo-sgAU9vvPf4LdB6M9l9wRfPhc"
BANCO_NAME = "BANCO.xlsx"
BUSCA_PREFIX = "BUSCA ZPS141 - "

# Sheets (timestamp)
ZPS_SPREADSHEET_ID = "1gDktQhF0WIjfAX76J2yxQqEeeBsSfMUPGs5svbf9xGM"
ZPS_SHEET_NAME = "zps"
TIMESTAMP_CELL = "K2"

# BANCO: no seu cenário, 1 linha de cabeçalho (linha 1)
HEADER_ROWS_BANCO = 1

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]


# ===================== AUTENTICAÇÃO =====================

def get_credentials():
    """
    No GitHub Actions: usar secret GOOGLE_CREDENTIALS (JSON do service account).
    Local: você pode opcionalmente ter um credenciais.json e setar GOOGLE_CREDENTIALS_FILE.
    """
    creds_json = os.getenv("GOOGLE_CREDENTIALS", "").strip()
    creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "").strip()

    if creds_json:
        info = json.loads(creds_json)
        return Credentials.from_service_account_info(info, scopes=SCOPES)

    if creds_file:
        return Credentials.from_service_account_file(creds_file, scopes=SCOPES)

    # fallback local (se você quiser manter compatibilidade)
    if os.path.exists("credenciais.json"):
        return Credentials.from_service_account_file("credenciais.json", scopes=SCOPES)

    raise RuntimeError(
        "Credenciais não encontradas. Defina o secret/env GOOGLE_CREDENTIALS (JSON) "
        "ou GOOGLE_CREDENTIALS_FILE apontando para um arquivo."
    )


def get_drive_service(creds):
    return build("drive", "v3", credentials=creds)


def get_gspread_client(creds):
    return gspread.authorize(creds)


# ===================== FUNÇÕES AUXILIARES =====================

def listar_arquivos_pasta(drive_service, folder_id):
    """Lista arquivos da pasta (paginado, inclui allDrives)."""
    query = f"'{folder_id}' in parents and trashed = false"
    files = []
    page_token = None

    while True:
        resp = drive_service.files().list(
            q=query,
            corpora="allDrives",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            fields="nextPageToken, files(id, name, mimeType, modifiedTime)",
            pageSize=1000,
            pageToken=page_token,
        ).execute()

        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return files


def encontrar_busca_mais_recente(arquivos, prefixo):
    """
    Entre os arquivos da pasta, encontra o BUSCA ZPS141 - DD.MM.AAAA mais recente.
    Usa a data do NOME (DD.MM.AAAA) para decidir.
    """
    mais_recente = None
    data_mais_recente = None

    for f in arquivos:
        nome = f.get("name", "")
        if not nome.startswith(prefixo):
            continue

        resto = nome[len(prefixo):].strip()
        data_str = resto[:10]  # "DD.MM.AAAA"
        try:
            dt = datetime.strptime(data_str, "%d.%m.%Y")
        except ValueError:
            continue

        if data_mais_recente is None or dt > data_mais_recente:
            data_mais_recente = dt
            mais_recente = f

    return mais_recente


def baixar_arquivo_excel(drive_service, file_id):
    """Baixa um arquivo do Drive como binário (excel)."""
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False

    while not done:
        status, done = downloader.next_chunk()
        if status:
            print(f"    ⬇️ Download: {int(status.progress() * 100)}%")

    fh.seek(0)
    return fh


def atualizar_arquivo_excel(drive_service, file_id, df, sheet_name):
    """
    Sobrescreve o arquivo Excel no Drive com um único sheet contendo df.
    df já contém o cabeçalho como linha de dados (header=False).
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False, header=False)

    output.seek(0)

    media = MediaIoBaseUpload(
        output,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        resumable=True,
    )

    drive_service.files().update(
        fileId=file_id,
        media_body=media,
        supportsAllDrives=True,
    ).execute()


def registrar_timestamp_zps(gspread_client):
    """Atualiza a célula K2 da aba 'zps' com timestamp atual."""
    sh = gspread_client.open_by_key(ZPS_SPREADSHEET_ID)
    ws = sh.worksheet(ZPS_SHEET_NAME)
    agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    ws.update_acell(TIMESTAMP_CELL, agora)


# ===================== LÓGICA PRINCIPAL =====================

def main():
    creds = get_credentials()
    drive_service = get_drive_service(creds)
    gs_client = get_gspread_client(creds)

    print("🔄 Iniciando compilador ZPS141 (GitHub Actions ready)...")

    arquivos = listar_arquivos_pasta(drive_service, FOLDER_ID)
    if not arquivos:
        raise RuntimeError("Nenhum arquivo encontrado na pasta informada.")

    # Localizar BANCO.xlsx
    arquivo_banco = next((f for f in arquivos if f.get("name") == BANCO_NAME), None)
    if not arquivo_banco:
        raise RuntimeError(f"Arquivo '{BANCO_NAME}' não encontrado na pasta.")

    # Localizar BUSCA ZPS141 mais recente pelo nome
    arquivo_hoje = encontrar_busca_mais_recente(arquivos, BUSCA_PREFIX)
    if not arquivo_hoje:
        raise RuntimeError(f"Nenhum arquivo do tipo '{BUSCA_PREFIX}DD.MM.AAAA' encontrado na pasta.")

    print(f"📂 Arquivo do dia mais recente: {arquivo_hoje['name']} ({arquivo_hoje['id']})")
    print(f"📂 BANCO: {arquivo_banco['name']} ({arquivo_banco['id']})")

    # Baixar ambos
    print("⬇️ Baixando arquivo do dia...")
    fh_hoje = baixar_arquivo_excel(drive_service, arquivo_hoje["id"])

    print("⬇️ Baixando BANCO.xlsx...")
    fh_banco = baixar_arquivo_excel(drive_service, arquivo_banco["id"])

    # Detectar abas
    xls_banco = pd.ExcelFile(fh_banco)
    sheet_banco = xls_banco.sheet_names[0]
    print(f"📄 Aba detectada no BANCO.xlsx: {sheet_banco}")

    xls_hoje = pd.ExcelFile(fh_hoje)
    sheet_hoje = xls_hoje.sheet_names[0]
    print(f"📄 Aba detectada no arquivo do dia: {sheet_hoje}")

    # Ler sem header
    df_banco = pd.read_excel(xls_banco, sheet_name=sheet_banco, header=None)
    df_hoje = pd.read_excel(xls_hoje, sheet_name=sheet_hoje, header=None)

    print(f"   ➜ BANCO: {df_banco.shape[0]} linhas x {df_banco.shape[1]} colunas")
    print(f"   ➜ HOJE:  {df_hoje.shape[0]} linhas x {df_hoje.shape[1]} colunas")

    # BANCO: filtrar col N (índice 13) começando com Y (somente dados)
    if df_banco.shape[1] <= 13:
        raise RuntimeError("BANCO não possui coluna N (índice 13). Verifique a estrutura do BANCO.xlsx.")

    print("🧹 Limpando BANCO: mantendo apenas linhas onde N começa com 'Y' (dados, não cabeçalho)...")

    header_banco = df_banco.iloc[:HEADER_ROWS_BANCO, :].copy()
    dados_banco = df_banco.iloc[HEADER_ROWS_BANCO:, :].copy()

    colN = dados_banco.iloc[:, 13].astype(str)
    mask_keep = colN.str.startswith("Y", na=False)
    removidas = int((~mask_keep).sum())

    dados_banco_filtrado = dados_banco[mask_keep].copy()
    df_banco = pd.concat([header_banco, dados_banco_filtrado], ignore_index=True)

    print(f"   ➜ Linhas removidas (N não começa com 'Y'): {removidas}")
    print(f"   ➜ Linhas finais no BANCO (incl. cabeçalho): {len(df_banco)}")

    # Preparar dados B6:AJ (linhas a partir da 6 => índice 5; colunas B..AJ => 1..35)
    print("📋 Preparando dados de B6:AJ do arquivo do dia...")
    dados_hoje = df_hoje.iloc[5:, 1:36].copy()

    dados_hoje = dados_hoje.replace("", pd.NA)
    dados_hoje = dados_hoje[~dados_hoje.isna().all(axis=1)].copy()

    print(f"   ➜ Linhas copiadas de B6:AJ: {len(dados_hoje)} | Colunas efetivas: {dados_hoje.shape[1]}")

    # Append
    if not dados_hoje.empty:
        dados_hoje.columns = range(dados_hoje.shape[1])

        if df_banco.shape[1] < dados_hoje.shape[1]:
            extra = dados_hoje.shape[1] - df_banco.shape[1]
            for i in range(extra):
                df_banco[df_banco.shape[1] + i] = pd.NA

        antes = len(df_banco)
        df_banco = pd.concat([df_banco, dados_hoje], ignore_index=True)
        depois = len(df_banco)
        print(f"   ➜ Append OK: {antes} → {depois} linhas (incl. cabeçalho)")
    else:
        print("   ⚠️ Nada para append (B6:AJ vazio).")

    # Deduplicação ignorando A,C,D,U (0,2,3,20)
    print("🧹 Removendo duplicatas no BANCO (ignorando colunas A, C, D e U)...")
    header_banco = df_banco.iloc[:HEADER_ROWS_BANCO, :].copy()
    dados_banco = df_banco.iloc[HEADER_ROWS_BANCO:, :].copy()

    if not dados_banco.empty:
        colunas_todas = list(dados_banco.columns)
        subset = [c for c in colunas_todas if c not in (0, 2, 3, 20)]

        antes_dup = len(dados_banco)
        dados_banco_sem_dup = dados_banco.drop_duplicates(subset=subset, keep="first")
        depois_dup = len(dados_banco_sem_dup)

        df_banco = pd.concat([header_banco, dados_banco_sem_dup], ignore_index=True)

        print(f"   ➜ Dados: {antes_dup} → {depois_dup} (removidas {antes_dup - depois_dup} duplicatas)")
    else:
        print("   ➜ Sem dados para deduplicar.")

    # Atualizar BANCO.xlsx
    print("💾 Atualizando BANCO.xlsx no Drive...")
    atualizar_arquivo_excel(drive_service, arquivo_banco["id"], df_banco, sheet_name=sheet_banco)

    # Timestamp
    print(f"⏱  Gravando timestamp em {ZPS_SHEET_NAME}!{TIMESTAMP_CELL}...")
    registrar_timestamp_zps(gs_client)

    print("✅ Processo concluído com sucesso!")


if __name__ == "__main__":
    main()
