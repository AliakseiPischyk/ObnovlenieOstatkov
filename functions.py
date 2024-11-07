import random
import pandas as pd

def is_one_file(found_files_list, txt):
    if len(found_files_list) == 0:
        print('Файл ' + txt + ' отсутствует, обработки не будет')
        return False
    elif len(found_files_list) > 1:
        print('Найдено более 1 файла ' + txt + 'Обработки не будет')
        return False
    else:
        print('Обработка файла ' + txt)
        return True


def info_new_katrochki(len, txt):
    if len>0:
        print('Появились ' + str(len) + ' НОВЫЕ КАРТОЧКИ у ' + txt )
    else:
        print('Новых карточек у ' + txt + ' нет')

def info_price_proccessed(txt):
    print(txt+' успешно обработан')

def info_price_not_proccessed(txt):
    print(txt+' не обработан!')


def fill_null_with_0_else_randint(x):
    if pd.isnull(x):
        return 0
    else:
        return random.randint(500, 1000)


def fill_alpaka_ostatok(x):
    if x == 0:
        return 0
    elif x>0:
        return random.randint(500,1000)
    else:
        return -999

def get_text_before_1st_minus_sign(x):
    if '-' in x:
        return x.split('-')[0]
    else:
        return x

def add_our_fbs_stock(row, our_fbs_stock):
    if row['Артикул'] in our_fbs_stock['Артикул'].values:
        kolvo = our_fbs_stock[row['Артикул'] == our_fbs_stock['Артикул']]['Количество'].iloc[0]
        b = row['Количество'] + kolvo
        return b
    else:
        return row['Количество']

def rename_barcode_col(df):
    df.rename(columns={'Штрих-код': 'Штрих код', 'Штрих - код': 'Штрих код',
                            'Штрихкод': 'Штрих код', 'Штрих код': 'Штрих код'}, inplace=True)
                            #на всякий случай перебарл варики, а то они руками пишут,
                            #может быть и ошибки в названии других колонок
    return df