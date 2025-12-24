from ftplib import FTP
import ftplib
import os
import time
import subprocess

# Configurações
ftp_server = 'ftp.mtps.gov.br'
ftp_directory = '/pdet/microdados/NOVO CAGED'
local_year_month_file = 'anos_meses_registrados.txt'
log_file = 'log_BuscaCaged.txt'
download_directory = 'CAGEDMOV_downloads'  # Diretório onde os arquivos serão salvos
retry_interval = 10  # Tempo em segundos antes de tentar novamente em caso de falha

def registrar_log(mensagem):
    with open(log_file, 'a') as f:
        f.write(mensagem + '\n')

def obter_anos_meses_registrados(file_path):
    if not os.path.exists(file_path):
        if file_path == local_year_month_file:
            registrar_log("Arquivo de anos e meses registrados não encontrado. Criando um novo.")
        return {}
    with open(file_path, 'r') as f:
        data = {}
        for line in f:
            year, months = line.strip().split(' - ')
            data[year] = months.split(',')
        return data

def salvar_ano_mes_registrado(file_path, data):
    with open(file_path, 'w') as f:
        for year, months in data.items():
            months_str = ','.join(months)
            f.write(f"{year} - {months_str}\n")

# Função para criar o diretório de download, se não existir
os.makedirs(download_directory, exist_ok=True)
if not os.path.exists(log_file):
    registrar_log("Diretorio: CAGEDMOV_downloads não foi encontrado, criarei em minha raiz uma pasta com este mesmo nome")

# Tentativas de conexão
while True:
    try:
        # Conectar ao FTP
        ftp = FTP(ftp_server)
        ftp.login()
        ftp.set_pasv(True)

        # Verificar o caminho FTP atual e listar diretórios
        print("Diretório atual:", ftp.pwd())
        print("Listando diretórios:")
        ftp.retrlines('LIST')

        # Mudar para o diretório desejado
        try:
            ftp.cwd(ftp_directory)
        except ftplib.error_perm as e:
            registrar_log(f"Erro ao mudar para o diretório {ftp_directory}: {e}")
            ftp.quit()
            exit()

        # Obter lista de anos no diretório
        try:
            ftp.encoding = 'latin-1'
            anos_disponiveis = [name for name in ftp.nlst() if name.isdigit() and len(name) == 4]
            anos_disponiveis.sort()
            print("Diretório atual:", ftp.pwd())
            print("Listando diretórios:")
            ftp.retrlines('LIST')
        except UnicodeDecodeError as e:
            registrar_log(f"Erro ao listar anos disponíveis: {e}")
            ftp.quit()
            exit()

        # Verificar anos e meses registrados
        anos_meses_registrados = obter_anos_meses_registrados(local_year_month_file)
        novo_ano_encontrado = False
        ano_selecionado = None

        for ano in anos_disponiveis:
            if ano not in anos_meses_registrados:
                anos_meses_registrados[ano] = []  # Adiciona o novo ano
                registrar_log(f"Novo ano encontrado: {ano}")
                ftp.cwd(ano)
                meses_disponiveis = [name[4:] for name in ftp.nlst() if name.isdigit() and len(name) == 6 and name.startswith(ano)]
                meses_disponiveis.sort()
                for mes in meses_disponiveis:
                    ftp.cwd(f"{ano}{mes}")
                    arquivos = ftp.nlst()
                    arquivos_caged = [arq for arq in arquivos if arq.startswith('CAGEDMOV')]
                    if arquivos_caged:
                        registrar_log(f"Novo ano encontrado, e mês {mes} contém {len(arquivos_caged)} arquivo(s) CAGED.")
                        for arquivo_cagedmov in arquivos_caged:
                            local_file_path = os.path.join(download_directory, arquivo_cagedmov)
                            # Evita download duplicado
                            if os.path.exists(local_file_path):
                                registrar_log(f"Arquivo já existe, pulando: {arquivo_cagedmov}")
                                continue
                            registrar_log(f"Baixando: {arquivo_cagedmov} -> {local_file_path}")
                            with open(local_file_path, 'wb') as local_file:
                                ftp.retrbinary(f'RETR {arquivo_cagedmov}', local_file.write)
                            registrar_log(f"Arquivo {arquivo_cagedmov} baixado com sucesso para {local_file_path}.")
                        # Atualizar o registro de meses após baixar todos
                        if mes not in anos_meses_registrados[ano]:
                            anos_meses_registrados[ano].append(mes)
                            salvar_ano_mes_registrado(local_year_month_file, anos_meses_registrados)
                    else:
                        registrar_log(f"Novo ano encontrado, mas o mês {mes} não continha nenhum arquivo CAGED.")
                    ftp.cwd("..")
                anos_meses_registrados[ano] = meses_disponiveis
                salvar_ano_mes_registrado(local_year_month_file, anos_meses_registrados)
                novo_ano_encontrado = True
                ano_selecionado = ano
                registrar_log(f"Novo ano {ano} e meses adicionados: {','.join(meses_disponiveis)}")
                ftp.cwd("..")
            else:
                ftp.cwd(f"{ano}")
                meses_disponiveis = [name[4:] for name in ftp.nlst() if name.isdigit() and len(name) == 6 and name.startswith(ano)]
                meses_disponiveis.sort()
                meses_registrados = set(anos_meses_registrados[ano])
                novos_meses = [mes for mes in meses_disponiveis if mes not in meses_registrados]    
                for mes in novos_meses:
                    ftp.cwd(f"{ano}{mes}")
                    arquivos = ftp.nlst()
                    arquivos_caged = [arq for arq in arquivos if arq.startswith('CAGEDMOV')]
                    if arquivos_caged:
                        registrar_log(f"Novos meses encontrados, e mês {mes} contém {len(arquivos_caged)} arquivo(s) CAGED.")
                        for arquivo_cagedmov in arquivos_caged:
                            local_file_path = os.path.join(download_directory, arquivo_cagedmov)
                            if os.path.exists(local_file_path):
                                registrar_log(f"Arquivo já existe, pulando: {arquivo_cagedmov}")
                                continue
                            registrar_log(f"Baixando: {arquivo_cagedmov} -> {local_file_path}")
                            with open(local_file_path, 'wb') as local_file:
                                ftp.retrbinary(f'RETR {arquivo_cagedmov}', local_file.write)
                            registrar_log(f"Arquivo {arquivo_cagedmov} baixado com sucesso para {local_file_path}.")
                        # Atualiza registro após baixar todos os arquivos do mês
                        anos_meses_registrados[ano].append(mes)
                        salvar_ano_mes_registrado(local_year_month_file, anos_meses_registrados)
                    else:
                        registrar_log(f"Novo ano encontrado, mas o mês {mes} não continha nenhum arquivo CAGED.")
                    ftp.cwd("..")

                ftp.cwd("..")

            if novo_ano_encontrado:
                break

        if not novo_ano_encontrado:
            ano_selecionado = max(anos_meses_registrados.keys(), key=int)

        # Selecionar o último mês registrado ou o mais recente
        ultimo_mes_registrado = max(anos_meses_registrados[ano_selecionado], key=int)
        ftp.cwd(ano_selecionado)

        meses_disponiveis = [name[4:] for name in ftp.nlst() if name.isdigit() and len(name) == 6 and name.startswith(ano_selecionado)]
        meses_disponiveis.sort()

        mes_mais_recente = max(meses_disponiveis, key=int) if meses_disponiveis else None
        print(f"mes: {mes_mais_recente}")
        if not mes_mais_recente:
            registrar_log(f"Nenhum mês disponível para o ano {ano_selecionado}.")
            ftp.quit()
            exit()

        if mes_mais_recente in anos_meses_registrados[ano_selecionado]:
            registrar_log(f"Mês {mes_mais_recente} já está atualizado.")
        else:
            registrar_log(f"Novo mês encontrado: {mes_mais_recente}. Iniciando download.")
            ftp.cwd(f"{ano_selecionado}{mes_mais_recente}")

            # Baixar o arquivo CAGEDMOV
            arquivos = ftp.nlst()
            arquivos_caged = [arq for arq in arquivos if arq.startswith('CAGEDMOV')]

            if arquivos_caged:
                for arquivo_cagedmov in arquivos_caged:
                    local_file_path = os.path.join(download_directory, arquivo_cagedmov)
                    if os.path.exists(local_file_path):
                        registrar_log(f"Arquivo já existe, pulando: {arquivo_cagedmov}")
                        continue
                    registrar_log(f"Tentando baixar o arquivo: {arquivo_cagedmov} para o caminho: {local_file_path}")
                    with open(local_file_path, 'wb') as local_file:
                        ftp.retrbinary(f'RETR {arquivo_cagedmov}', local_file.write)
                    registrar_log(f"Arquivo {arquivo_cagedmov} baixado com sucesso para {local_file_path}.")
                # Atualizar o registro de meses após o download bem-sucedido
                if mes_mais_recente not in anos_meses_registrados[ano_selecionado]:
                    anos_meses_registrados[ano_selecionado].append(mes_mais_recente)
                    salvar_ano_mes_registrado(local_year_month_file, anos_meses_registrados)
            else:
                registrar_log("Nenhum arquivo CAGEDMOV encontrado no diretório.")

        # Fechar a conexão FTP
        ftp.quit()
        # Tentar chamar o script converterCSV.py
        try:
            subprocess.run(['python', 'converterCSV.py'], check=True)
            registrar_log("Script converterCSV.py executado com sucesso.")
        except FileNotFoundError:
            registrar_log("Erro: Script converterCSV.py não encontrado.")
        except subprocess.CalledProcessError as e:
            registrar_log(f"Erro ao executar o script converterCSV.py: {e}")
        break

    except ConnectionRefusedError as e:
        registrar_log(f"Erro de conexão (10061): {e}. Tentando novamente em {retry_interval} segundos.")
        time.sleep(retry_interval)
    except Exception as e:
        registrar_log(f"Erro inesperado: {e}")
        ftp.quit()
        exit()
