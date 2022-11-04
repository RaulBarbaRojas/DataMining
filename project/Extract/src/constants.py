''' MODULE FOR CONSTANT VALUES DEFINITION '''

LIST_MONTHS = [
    'Febrero',
    'Marzo',
    'Abril',
    'Mayo',
    'Junio',
    'Julio',
    'Agosto',
    'Septiembre',
    'Octubre',
    'Noviembre',
    'Diciembre'
]

LIST_PRODUCTS = [
    'TOTAL PATATAS',
    'PATATAS FRESCAS',
    'PATATAS CONGELADAS',
    'PATATAS PROCESADAS',
    'PATATAS FRITAS',
    'T.HORTALIZAS FRESCAS',
    'TOMATES',
    'CEBOLLAS',
    'AJOS',
    'COLES',
    'PEPINOS',
    'JUDIAS VERDES',
    'PIMIENTOS',
    'CHAMPIÑONES+O.SETAS',
    'LECHUGA/ESC./ENDIVIA',
    'ESPARRAGOS',
    'VERDURAS DE HOJA',
    'BERENJENAS',
    'ZANAHORIAS',
    'CALABACINES',
    'OTR.HORTALIZAS/VERD.',
    'BROCOLI',
    'ALCACHOFAS',
    'APIO',
    'COLIFLOR',
    'VERD./HORT. IV GAMA',
    'PUERRO',
    'T.FRUTAS FRESCAS',
    'NARANJAS',
    'MANDARINAS',
    'LIMONES',
    'PLATANOS',
    'MANZANAS',
    'PERAS',
    'MELOCOTONES',
    'NECTARINAS',
    'ALBARICOQUES',
    'FRESAS/FRESON',
    'MELON',
    'SANDIA',
    'CIRUELAS',
    'CEREZAS',
    'UVAS',
    'KIWI',
    'AGUACATE',
    'PIÑA',
    'OTRAS FRUTAS FRESCAS',
    'POMELO',
    'CHIRIMOYA',
    'MANGO',
    'FRUTAS IV GAMA'
]

DATASET1_COLUMNS = [
    'Producto',
    'CONSUMO X CAPITA',
    'GASTO X CAPITA',
    'PENETRACION (%)',
    'PRECIO MEDIO kg ó litros',
    'VALOR (Miles Euros)',
    'VOLUMEN (Miles kg ó litros)'
]

CCAA_POSITIONS = {
    'Cataluña' : 'A,H:M',
    'Aragon' : 'A,N:S',
    'Baleares' : 'A,T:Y',
    'Valencia' : 'A,Z:AE',
    'Murcia' : 'A,AF:AK',
    'Andalucia' : 'A,AL:AQ',
    'Madrid' : 'A,AR:AW',
    'Castilla La Mancha' : 'A,AX:BC',
    'Extremadura' : 'A,BD:BI',
    'Castilla Leon' : 'A,BJ:BO',
    'Galicia' : 'A,BP:BU',
    'Asturias' : 'A,BV:CA',
    'Cantabria' : 'A,CB:CG',
    'Pais Vasco' : 'A,CH:CM',
    'La Rioja' : 'A,CN:CS',
    'Navarra' : 'A,CT:CY',
    'Canarias' : 'A,CZ:DE'
}

DATASET1_2018_URL = 'https://www.mapa.gob.es/es/alimentacion/temas/consumo-tendencias/' + \
    '2018datosmensualesdelpaneldeconsumoalimentarioenhogares_tcm30-520451_tcm30-520451.xlsx'

DATASET1_2019_URL = 'https://www.mapa.gob.es/es/alimentacion/temas/consumo-tendencias/' +  \
    '2019datosmensualesdelpaneldeconsumoalimentarioenhogares_tcm30-5204501_tcm30-520450.xlsx'

DATASET1_2020_URL = 'https://www.mapa.gob.es/es/alimentacion/temas/consumo-tendencias/' + \
    '2020-datos-mensuales-panel-hogares-ccaa-rev-nov2021_tcm30-540244.xlsx'

DATASET5_URL = 'https://www.ecdc.europa.eu/sites/default/' \
    + 'files/documents/COVID-19-geographic-disbtribution-worldwide-2020-12-14.xlsx'

DATASET1_NAME = '/home/app/data/dataset1.txt'
DATASET5_NAME = '/home/app/data/dataset5.txt'

ROWS_TO_PARSE_2018_AND_2019 = 467
ROWS_TO_PARSE_2020 = 497
ROWS_TO_PARSE_DEC_2019 = 504
