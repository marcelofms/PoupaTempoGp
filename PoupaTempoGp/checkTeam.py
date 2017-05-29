'''

criado em 19/04/2017

'''

import sys
import pandas as pd
import os


def show_producao_per_recurso(dados_prod):
    print('Calculando produção individual...')
    '''
     calcula as proporções de valor de cada produto feito pelos recursos para
     agrupar por período (totalizado)
    '''
    # dataframe a ser preenchido
    df_producao_per_recurso = pd.DataFrame(columns=['Recurso',
                                                    'Projeto',
                                                    'OS',
                                                    'Fase',
                                                    'Valor_fase',
                                                    'Esforco_fase',
                                                    'Produto',
                                                    'Valor_produto',
                                                    'Esforco_produto',
                                                    'Entrega',
                                                    'Status'])

    df_fases = dados_prod.loc[dados_prod['Tipo'] == 'Fase']  # Lista todas as fases
    df_valores_fase = pd.DataFrame(columns=['Projeto', 'OS', 'Fase', 'Esforco', 'Valor', 'Entrega'])

    # preenche a relação de fases com os dados necessarios
    for index, i_fase in df_fases.iterrows():
        ''' calcula valor da fase '''
        num_os = i_fase['Tarefa principal']
        custo_folha = dados_prod.loc[i_fase['Tarefa principal'], 'Valor da NF-e'] * float(str(i_fase['Percentual da Fase']).replace(',', '.'))

        ''' Calcula esforço total (soma do esforço dos produtos da fase) '''
        df_prods = dados_prod.loc[(dados_prod['Tipo'] == 'Produto') & (dados_prod['Tarefa principal'] == index)] 
        esforco_total = 0

        if not df_prods.empty:
            for idx, i_prod in df_prods.iterrows():
                esforco_total += int(calc_duracao_tarefa(i_prod['Data de início'], i_prod['Data de fim']))

            df_valores_fase = df_valores_fase.append({'Projeto': i_fase['Projeto'],
                                    'OS': num_os,
                                    'Fase': index,
                                    'Esforco': esforco_total,
                                    'Valor': custo_folha,
                                    'Entrega': i_fase['Data de fim']
                                    }, ignore_index=True)
    df_valores_fase.fillna(0, inplace=True)
    
    
    ''' busca todos os recursos que figuram no planejamento '''
    df_recursos = dados_prod.loc[(dados_prod['Tipo'] == 'Produto') & (dados_prod['Estado'] != 'Cancelada')]  # apenas produtos para identificar a atribuição
        
    for id_rec, rec in df_recursos.groupby(['Atribuído a']):
        ''' busca todos os produtos do recurso '''
        df_prod_rec = dados_prod.loc[(dados_prod['Tipo'].isin(['Produto', 'Atividade'])) &
                                     (dados_prod['Atribuído a'] == id_rec) &
                                     (dados_prod['Estado'] != 'Cancelada')]

        ''' Apura o valor proporcional de cada produto '''
        
        vlr_prod = 0
        if not df_prod_rec.empty:
            for id_prd, prds in df_prod_rec.iterrows():
                esforco_prod = calc_duracao_tarefa(prds['Data de início'], prds['Data de fim'])
                
                df_fase_prod = df_valores_fase.loc[df_valores_fase['Fase'] == prds['Tarefa principal']]
                
                if not df_fase_prod.empty:
                    esf_fase = 0
                    val_fase = 0
                    num_os_fase = ''
                    for indx, fs in df_fase_prod.iterrows():
                        esf_fase += fs['Esforco']
                        val_fase += fs['Valor']
                        num_os_fase = fs['OS']

                    vlr_prod = (esforco_prod / esf_fase) * val_fase

                    df_producao_per_recurso = df_producao_per_recurso.append({'Recurso': id_rec,
                                                'Projeto': prds['Projeto'],
                                                'OS': num_os_fase,
                                                'Fase':prds['Tarefa principal'],
                                                'Valor_fase': "{0:.2f}".format(val_fase),
                                                'Esforco_fase': esf_fase,
                                                'Produto': id_prd,
                                                'Valor_produto': "{0:.2f}".format(vlr_prod),
                                                'Esforco_produto': esforco_prod,
                                                'Entrega': prds['Data de fim'],
                                                'Status': prds['Estado']
                                                }, ignore_index=True)
    
    ''' Publica produção por recurso detalhada '''
    df_producao_per_recurso.to_csv(str(get_path_output()) + '\producao_recursos_detalhe.csv', sep=';')

    ''' versão agrupada dos dados '''
    publica_prod_consolidada(df_producao_per_recurso)
    publica_prod_semanal_consolid(df_producao_per_recurso)
    publica_prod_consolidada_por_os(df_producao_per_recurso)


def publica_prod_consolidada_por_os(df_producao):
    print('Consolidando dados por OS...')

    df_periodos = pd.DataFrame(columns=['Periodo_entrega'])
    df_periodos['Periodo_entrega'] = df_producao['Entrega'].apply(lambda d: str(str(d.year) + '-' + str(d.month)))
    df_producao = pd.concat([df_producao, df_periodos], axis=1)

    ''' cria registro consolidado '''
    new_columns = ['Recurso', 'Projeto', 'OS', 'Periodo_entrega', 'Valor', 'Status']
    dados_consolid = pd.DataFrame(columns=new_columns)

    ''' Formata status dos itens de produção '''
    df_producao['Status'] = df_producao['Status'].apply(lambda s: 'Realizado' if s == 'Concluída' else 'Previsto')

    # consolida os valores por periodo
    for item, linha in df_producao.groupby(['Recurso', 'Projeto', 'OS', 'Periodo_entrega', 'Status']):
        # filtra os dados do projeto/periodo

        df_itens = df_producao.loc[(df_producao["Recurso"] == item[0]) &
                                   (df_producao["Projeto"] == item[1]) &
                                   (df_producao["OS"] == item[2]) &
                                   (df_producao["Periodo_entrega"] == item[3]) &
                                   (df_producao["Status"] == item[4])]

        val_consolidado = 0
        for index, it_periodo in df_itens.iterrows():
            val_consolidado += (float(it_periodo['Valor_produto']) * 100)

        # preenche o dataframe
        dados_consolid = dados_consolid.append({'Recurso': item[0],
                                                'Projeto': item[1],
                                                'OS': item[2],
                                                'Periodo_entrega': item[3],
                                                'Valor': val_consolidado / 100,
                                                'Status': item[4]
                                                }, ignore_index=True)

    dados_consolid.fillna(0, inplace=True)

    ''' consolidação da produção individual '''
    dados_consolid.to_csv(str(get_path_output()) + '\prod_rec_consolid_os.csv', sep=';')

    ''' convert/pivot para plotagem '''

    dados_prod_os_real = pd.DataFrame(columns=['Recurso', 'Periodo_entrega', 'OS', 'Valor'])
    dados_prod_os_prev = pd.DataFrame(columns=['Recurso', 'Periodo_entrega', 'OS', 'Valor'])

    for ix, it in dados_consolid.groupby(['Recurso', 'Periodo_entrega', 'OS']):
        vlr_prd_real = 0
        vlr_prd_prev = 0
        prod_it = dados_consolid.loc[(dados_consolid['Recurso'] == ix[0]) &
                                     (dados_consolid['Periodo_entrega'] == ix[1]) &
                                     (dados_consolid['OS'] == ix[2])]
        for id_prd, it_prd in prod_it.iterrows():
            if it_prd['Status'] == 'Realizado':
                vlr_prd_real += it_prd['Valor']
            else:
                vlr_prd_prev += it_prd['Valor']

        dados_prod_os_real = dados_prod_os_real.append({'Recurso': ix[0],
                                                        'Periodo_entrega': ix[1],
                                                        'OS': ix[2],
                                                      'Valor': vlr_prd_real
                                                       }, ignore_index=True)
        dados_prod_os_prev = dados_prod_os_prev.append({'Recurso': ix[0],
                                                        'Periodo_entrega': ix[1],
                                                        'OS': ix[2],
                                                        'Valor': vlr_prd_prev
                                                       }, ignore_index=True)

    # prov_pivot_por_os_no_mes(dados_prod_os_real, '2017-5', 'real')
    dados_prod_os_real = pd.pivot_table(dados_prod_os_real, index=['Recurso', 'OS'], columns='Periodo_entrega')
    dados_prod_os_real.fillna(0, inplace=True)
    dados_prod_os_real.to_csv(str(get_path_output()) + '\prod_os_rec_real.csv', sep=';')

    # prov_pivot_por_os_no_mes(dados_prod_os_prev, '2017-5', 'prev')
    dados_prod_os_prev = pd.pivot_table(dados_prod_os_prev, index=['Recurso', 'OS'], columns='Periodo_entrega')
    dados_prod_os_prev.fillna(0, inplace=True)
    dados_prod_os_prev.to_csv(str(get_path_output()) + '\prod_os_rec_prev.csv', sep=';')


def prov_pivot_por_os_no_mes(df_dados, mes, tipo):
    print('Dados do PIVOT do Mês')
    dados_os_mes = df_dados.loc[(df_dados["Periodo_entrega"] == mes)]
    dados_os_mes = pd.pivot_table(dados_os_mes, index=['Recurso'], columns='OS')
    dados_os_mes.fillna(0, inplace=True)

    titulo_arq = str('\prod_os_rec_' + tipo + '_' + mes + '.csv')

    dados_os_mes.to_csv(str(get_path_output()) + titulo_arq, sep=';')


def publica_prod_consolidada(df_producao):
    print('Consolidando dados...')
    
    df_periodos = pd.DataFrame(columns=['Periodo_entrega'])
    df_periodos['Periodo_entrega'] = df_producao['Entrega'].apply(lambda d: str(str(d.year) + '-' + str(d.month)))
    df_producao = pd.concat([df_producao, df_periodos], axis=1)

    ''' cria registro consolidado '''
    new_columns = ['Recurso', 'Projeto', 'Periodo_entrega', 'Valor', 'Status']
    dados_consolid = pd.DataFrame(columns=new_columns)

    ''' Formata status dos itens de produção '''
    df_producao['Status'] = df_producao['Status'].apply(lambda s: 'Realizado' if s == 'Concluída' else 'Previsto')

    # consolida os valores por periodo
    for item, linha in df_producao.groupby(['Recurso', 'Projeto', 'Periodo_entrega', 'Status']):
        # filtra os dados do projeto/periodo

        df_itens = df_producao.loc[(df_producao["Recurso"] == item[0]) & 
                                   (df_producao["Projeto"] == item[1]) & 
                                   (df_producao["Periodo_entrega"] == item[2]) & 
                                   (df_producao["Status"] == item[3])]

        val_consolidado = 0
        for index, it_periodo in df_itens.iterrows():
            val_consolidado += (float(it_periodo['Valor_produto'])*100)

        # preenche o dataframe
        dados_consolid = dados_consolid.append({'Recurso': item[0],
                                                'Projeto': item[1],
                                                'Periodo_entrega': item[2],
                                                'Valor': val_consolidado/100,
                                                'Status': item[3]
        }, ignore_index=True)

    dados_consolid.fillna(0, inplace=True)    
    
    ''' consolidação da produção individual '''
    dados_consolid.to_csv(str(get_path_output()) + '\prod_rec_consolid.csv', sep=';')
    
    ''' convert/pivot para plotagem '''

    dados_prod_real = pd.DataFrame(columns=['Recurso', 'Periodo_entrega', 'Valor'])
    dados_prod_prev = pd.DataFrame(columns=['Recurso', 'Periodo_entrega', 'Valor'])

    for ix, it in dados_consolid.groupby(['Recurso','Periodo_entrega']):
        vlr_prd_real = 0
        vlr_prd_prev = 0
        prod_it = dados_consolid.loc[(dados_consolid['Recurso'] == ix[0]) & 
                                     (dados_consolid['Periodo_entrega'] == ix[1])]
        for id_prd, it_prd in prod_it.iterrows():
            if it_prd['Status'] == 'Realizado':
               vlr_prd_real += it_prd['Valor']
            else:
               vlr_prd_prev += it_prd['Valor']

        dados_prod_real = dados_prod_real.append({'Recurso': ix[0],
                                                  'Periodo_entrega': ix[1],
                                                  'Valor': vlr_prd_real
                                                  }, ignore_index=True)
        dados_prod_prev = dados_prod_prev.append({'Recurso': ix[0],
                                                  'Periodo_entrega': ix[1],
                                                  'Valor': vlr_prd_prev
                                                  }, ignore_index=True)

    dados_prod_real = dados_prod_real.pivot(index='Recurso', columns='Periodo_entrega', values='Valor')
    dados_prod_real.fillna(0, inplace=True)
    dados_prod_real.to_csv(str(get_path_output()) + '\prod_rec_real.csv', sep=';')

    dados_prod_prev = dados_prod_prev.pivot(index='Recurso', columns='Periodo_entrega', values='Valor')
    dados_prod_prev.fillna(0, inplace=True)
    dados_prod_prev.to_csv(str(get_path_output()) + '\prod_rec_prev.csv', sep=';')


def publica_prod_semanal_consolid(df_producao):
    print('Consolidando dados semanais...')

    df_periodos = pd.DataFrame(columns=['Periodo_entrega'])
    df_periodos['Periodo_entrega'] = df_producao['Entrega'].apply(lambda d: str(str(d.year) + '-' + str(d.month) +
                                                                                ' - S' + str(d.strftime('%V'))))
    df_producao = pd.concat([df_producao, df_periodos], axis=1)

    ''' cria registro consolidado '''
    new_columns = ['Recurso', 'Projeto', 'Periodo_entrega', 'Valor', 'Status']
    dados_consolid = pd.DataFrame(columns=new_columns)

    ''' Formata status dos itens de produção '''
    df_producao['Status'] = df_producao['Status'].apply(lambda s: 'Realizado' if s == 'Concluída' else 'Previsto')

    # consolida os valores por periodo
    for item, linha in df_producao.groupby(['Recurso', 'Projeto', 'Periodo_entrega', 'Status']):
        # filtra os dados do projeto/periodo

        df_itens = df_producao.loc[(df_producao["Recurso"] == item[0]) &
                                   (df_producao["Projeto"] == item[1]) &
                                   (df_producao["Periodo_entrega"] == item[2]) &
                                   (df_producao["Status"] == item[3])]

        val_consolidado = 0
        for index, it_periodo in df_itens.iterrows():
            val_consolidado += (float(it_periodo['Valor_produto']) * 100)

        # preenche o dataframe
        dados_consolid = dados_consolid.append({'Recurso': item[0],
                                                'Projeto': item[1],
                                                'Periodo_entrega': item[2],
                                                'Valor': val_consolidado / 100,
                                                'Status': item[3]
                                                }, ignore_index=True)

    dados_consolid.fillna(0, inplace=True)

    ''' consolidação da produção individual '''
    dados_consolid.to_csv(str(get_path_output()) + '\prod_rec_consolid.csv', sep=';')

    ''' convert/pivot para plotagem '''

    dados_prod_real = pd.DataFrame(columns=['Recurso', 'Periodo_entrega', 'Valor'])
    dados_prod_prev = pd.DataFrame(columns=['Recurso', 'Periodo_entrega', 'Valor'])

    for ix, it in dados_consolid.groupby(['Recurso', 'Periodo_entrega']):
        vlr_prd_real = 0
        vlr_prd_prev = 0
        prod_it = dados_consolid.loc[(dados_consolid['Recurso'] == ix[0]) &
                                     (dados_consolid['Periodo_entrega'] == ix[1])]
        for id_prd, it_prd in prod_it.iterrows():
            if it_prd['Status'] == 'Realizado':
                vlr_prd_real += it_prd['Valor']
            else:
                vlr_prd_prev += it_prd['Valor']

        dados_prod_real = dados_prod_real.append({'Recurso': ix[0],
                                                  'Periodo_entrega': ix[1],
                                                  'Valor': vlr_prd_real
                                                  }, ignore_index=True)
        dados_prod_prev = dados_prod_prev.append({'Recurso': ix[0],
                                                  'Periodo_entrega': ix[1],
                                                  'Valor': vlr_prd_prev
                                                  }, ignore_index=True)

    dados_prod_real = dados_prod_real.pivot(index='Recurso', columns='Periodo_entrega', values='Valor')
    dados_prod_real.fillna(0, inplace=True)
    dados_prod_real.to_csv(str(get_path_output()) + '\prod_semanal_rec_real.csv', sep=';')

    dados_prod_prev = dados_prod_prev.pivot(index='Recurso', columns='Periodo_entrega', values='Valor')
    dados_prod_prev.fillna(0, inplace=True)
    dados_prod_prev.to_csv(str(get_path_output()) + '\prod_semanal_rec_prev.csv', sep=';')


def calc_duracao_tarefa(dt_ini_tar, dt_fim_tar):
    if ((str(dt_ini_tar) and str(dt_fim_tar)) and
        (str(dt_fim_tar) != 'NaN') and
        (str(dt_fim_tar) != 'NaN') and
        (str(dt_ini_tar) != 'NaT') and
        (str(dt_fim_tar) != 'NaT')
    ):
        dti_dur_tarefa = pd.bdate_range(dt_ini_tar, dt_fim_tar)
        duracao_dias = dti_dur_tarefa.size
    elif not dt_fim_tar:
        duracao_dias = 1
    else:
        duracao_dias = 0

    if not str(duracao_dias).isnumeric:
        duracao_dias = 0 

    return duracao_dias


def main(argv):
    print(':::Início da execução:::')
    print('Parametros de execução:', argv)


    if str(argv).find('csv'):
        # calcula estatisticas do time
        print('Analisando equipes...')
        dados_prod = get_dados_faturamento_redmine(sys.argv[1])
        show_producao_per_recurso(dados_prod)
    else:
        print('Arquivo de origem dos dados não informado!')
        print('Indique o caminho completo para o arquivo CSV a ser importado.')
    print(':::Fim da Execução:::')


def get_dados_faturamento_redmine(arquivo):
    '''
        retorna um objeto DATAFRAME (Pandas) para pesquisa e manipulação
        arquivo - caminho completo para o arquivo CSV
    '''
    meus_dados = pd.read_csv(arquivo, sep=';', encoding='latin-1', header=0, index_col=0)
    meus_dados.sort_values(['Projeto'])
    # Ajusta as datas de planejamento
    meus_dados['Data de início'] = pd.to_datetime(meus_dados['Data de início'], format='%d/%m/%Y', errors='ignore')
    meus_dados['Data de fim'] = pd.to_datetime(meus_dados['Data de fim'], format='%d/%m/%Y', errors='ignore')

    # tratamento de dados usados nos cálculos
    for index, reg in meus_dados.iterrows():
        # Trata os percentuais inválidos (apenas para estimar valores e evitar distorçoes)
        val_teste = str(reg['Percentual da Fase']).replace(',', '.')
        if float(val_teste) > 1:
            meus_dados.set_value(index, 'Percentual da Fase', str((float(val_teste) / 100)).replace('.', ','))

        # Trata o valor da Nota para a OS
        if str(reg['Tipo']).find('Ordem') >= 0:
            if str(reg['Valor da NF-e']).strip() == '':
                meus_dados.set_value(index, 'Valor da NF-e', '0.0')


    # remove valores inválidos
    meus_dados.fillna({'Valor da NF-e': 0}, inplace=True)

    # Prepara o campo de ordenação por data
    datas_ord = meus_dados['Data de início']
    datas_ord.rename(columns={'Data de início': 'Dt_ini_ord'}, inplace=True)
    datas_ord.apply(lambda d: str(str(d.year) + '-' + str(d.month) + '-' + str(d.day)))

    # merge dos dados
    meus_dados = pd.concat([meus_dados, datas_ord], axis=1)
    meus_dados.rename(columns={0: 'Dt_ini_ord'}, inplace=True)
    return meus_dados


def get_path_output():
    # testa se a app é script ou frozen exe
    if getattr(sys, 'frozen', False):
        path_dir_saida = os.path.dirname(sys.executable)
    elif __file__:
        path_dir_saida = os.path.dirname(__file__)

    # trata a execução em desenv
    path_dir_saida = str(path_dir_saida).replace('\\src', '')
    return str(path_dir_saida)

if __name__=='__main__':
    main(sys.argv)
