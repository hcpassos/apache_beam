import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.core import Map

pipeline_options = PipelineOptions(arg=None)
pipeline = beam.Pipeline(options=pipeline_options)

colunas_dengue = [
                'id',
                'data_iniSE',
                'casos',
                'ibge_code',
                'cidade',
                'uf',
                'cep',
                'latitude',
                'longitude']


def lista_para_dicionario(elemento, colunas_dengue):
    """
    Recebe uma linha da lista
    Retorna um dicionário
    """
    return dict(zip(colunas_dengue, elemento))


def texto_para_lista(elemento, delimitador='|'):
    """
    Recebe um texto e um delimitador
    Retorna uma lista de elementos pelo delimitador
    """
    return elemento.split(delimitador)


def trata_datas(elemento):
    """
    Recebe um dicionário e cria um novo campo ANO-MES
    Retorna o mesmo dicionário com um novo campo
    """
    elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2])
    return elemento


def chave_uf(elemento):
    """
    Receber um dicionário
    Retornar uma tupla (UF,dict)
    """
    chave = elemento['uf']
    return (chave, elemento)


def casos_dengue(elemento):
    """
    Recebe uma tupla ('RS',[{},{}])
    Retornar uma tupla ('RS-2014-12',8.0)
    """
    uf, registros = elemento
    for registro in registros:
        if bool(re.search(r'\d',registro['casos'])):
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))
        else:
            yield (f"{uf}-{registro['ano_mes']}", 0.0)


def chave_uf_ano_mes_de_lista(elemento):
    """
    Receber uma lista de elementos
    REtornar uma tupla contendo uma chave e o valor de uma chuva
    em mm ('UF-ANO-MES',1.3)
    """
    data,mm,uf = elemento
    anomes = '-'.join(data.split('-')[:2])
    chave = f'{uf}-{anomes}'
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
    return chave,mm


def arredonda(elemento):
    """
    Recebe uma tupla
    Retorna uma tupla arredondada
    """
    chave, mm = elemento
    return (chave,round(mm,1))


def descompactar_elementos(elemen):
    chave, dados = elemen
    chuva = str(dados.get('chuvas')).replace('[','')
    dengue = str(dados.get('dengue')).replace('[','')
    uf, ano, mes = chave.split('-')
    return uf,ano,mes,str(chuva).replace(']',''),str(dengue).replace(']','')


def preparar_csv(elemento, delimitador=';'):
    return f"{delimitador}".join(elemento)

# pcollection

dengue = (
   pipeline
   | "Leitura do dataset de dengue" >>
   ReadFromText('sample_casos_dengue.txt', skip_header_lines=1)
   | "De texto para lista" >> beam.Map(texto_para_lista)
   | "De lista para dicionário" >>
   beam.Map(lista_para_dicionario, colunas_dengue)
   | "Criar campo ano-mes" >> beam.Map(trata_datas)
   | "Criar chave pelo estado" >> beam.Map(chave_uf)
   | "Agrupar por UF" >> beam.GroupByKey()
   | "Descompactar casos de dengue" >> beam.FlatMap(casos_dengue)
   | "Soma dos casos pela chave" >> beam.CombinePerKey(sum)
   #| "print" >> beam.Map(print)
)


chuvas = (
    pipeline
    | "leitura do dataset de chuvas" >>
    ReadFromText('sample_chuvas.csv',skip_header_lines=1)
    | "De texto para lista (chuvas)" >> beam.Map(texto_para_lista,delimitador=',')
    | "Criando a chave UF-ANO-MES" >> beam.Map(chave_uf_ano_mes_de_lista)
    | "Soma dos casos pela chave (chuva)" >> beam.CombinePerKey(sum)
    | "Arredonda resultados de chuvas" >> beam.Map(arredonda)
    #| "Mostrar resultados de chuvas" >> beam.Map(print)
)


resultado = (
    #(chuvas,dengue)
    #| "Empilha as pcols" >> beam.Flatten()
    #| "Agrupa as pcols" >> beam.GroupByKey()
    ({'chuvas': chuvas,'dengue': dengue})
    | "Mesclar cols" >> beam.CoGroupByKey()
    | "Descompactar elementos" >> beam.Map(descompactar_elementos)
    | "Preparar CSV" >> beam.Map(preparar_csv)
    #| "Mostrar resultados de chuvas" >> beam.Map(print)

)

header = 'UF;ANO;MES;CHUVA;DENGUE'

resultado | 'Criar arquivo CSV' >> WriteToText('resultado',file_name_suffix='.csv',header = header)

pipeline.run()
