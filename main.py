import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

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

def lista_para_dicionario(elemento,colunas_dengue):
    """
    Recebe uma linha da lista
    Retorna um dicionário
    """
    return dict(zip(colunas_dengue,elemento))



def texto_para_lista(elemento, delimitador='|'):
    """
    Recebe um texto e um delimitador 
    Retorna uma lista de elementos pelo delimitador
    """
    return elemento.split(delimitador)

#pcollection
dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> 
        ReadFromText('casos_dengue.txt',skip_header_lines=1)
    | "De texto para lista" >> beam.Map(texto_para_lista)
    | "De lista para dicionário" >> beam.Map(lista_para_dicionario,colunas_dengue)
    | "Mostrar resultador" >> beam.Map(print)
)

pipeline.run()