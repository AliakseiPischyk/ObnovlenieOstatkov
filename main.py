import numpy as np
import pandas as pd
import random
import os
import re

prefixed = [filename for filename in os.listdir('.') if filename.startswith("seller_products")]
seller_products = pd.read_csv(prefixed[0], sep=';')
seller_products['Артикул'] = seller_products.apply(lambda row: row['Артикул'][1:], axis=1)
seller_products['Остаток'] = (seller_products['Доступно к продаже по схеме FBO, шт.']
                              + seller_products['Доступно к продаже по схеме FBS, шт.'])

seller_products.rename(columns={'Barcode': 'Штрих код'}, inplace=True)
seller_products['Штрих код'] = seller_products['Штрих код'].fillna(0)
seller_products['Штрих код'] = seller_products['Штрих код'].astype("string")
seller_products.sort_values(by='Артикул', inplace=True)


def fuck(x):
    if '-' in x:
        return x.split('-')[0]
    else:
        return x


seller_products['Штрих код'] = seller_products['Штрих код'].apply(lambda x: fuck(x))

nashi_fbs_ostatki = pd.read_excel('Текущие остатки.xlsx')
nashi_fbs_ostatki = nashi_fbs_ostatki.fillna('-')
nashi_fbs_ostatki = nashi_fbs_ostatki[nashi_fbs_ostatki['Количество'] != 0]

alpaka_prefixed = [filename for filename in os.listdir('./prices') if filename.startswith("price_mi")]
alpaka_df = pd.read_excel('prices/' + alpaka_prefixed[0])

alpaka_df = alpaka_df.drop(alpaka_df.columns[[0, 2, 3, 4, 7, 8, 10, 11, 12]], axis=1)
alpaka_df.columns = alpaka_df.loc[3].values.flatten().tolist()
alpaka_df = alpaka_df.drop([0, 1, 2, 3], axis=0)
alpaka_df = alpaka_df[alpaka_df['Штрихкод'].notna()]

def lol(x):
    if x == 'Нет в наличии':
        return 0
    elif x == 'Мало' or x == 'В наличии':
        return random.randint(500, 1000)
    else:
        return -999


alpaka_df['Наличие'] = alpaka_df['Наличие'].apply(lambda x: lol(x))
alpaka_df.rename(columns={'Штрихкод': 'Штрих код'}, inplace=True)


alpaka_df = alpaka_df[alpaka_df['Наименование'].str.startswith(("Advance", 'NT','RIO'))
                      | alpaka_df['Наименование'].str.contains('GIMCAT|GIMСАT')] # "СА" русские во 2м варианте
alpaka_df_merged = alpaka_df.merge(seller_products, on='Штрих код', how='left')
alpaka_df_new = alpaka_df_merged[alpaka_df_merged['Артикул'].isna()]
alpaka_df_new.to_excel('new kartochki/alpaka.xlsx', index=False)
alpaka_df_merged = alpaka_df.merge(seller_products, on='Штрих код')

alpaka_stop_list = pd.read_excel('stop list/alpaka.xlsx', converters={'Штрих код': str})
alpaka_stop_list = alpaka_stop_list[alpaka_stop_list['Продаем(да,нет)'] == 'нет']
alpaka_df_merged['Наличие'] = alpaka_df_merged['Наличие']*(~alpaka_df_merged['Штрих код'].isin(alpaka_stop_list['Штрих код'])).astype(int)


alpaka_stock_template = pd.read_excel('stock-update-template.xlsx', sheet_name='Остатки на складе')
alpaka_stock_template['Артикул'] = alpaka_df_merged['Артикул']
alpaka_stock_template['Количество'] = alpaka_df_merged['Наличие']

def kek(row):
    if row['Артикул'] in nashi_fbs_ostatki['Артикул'].values:
        kolvo = nashi_fbs_ostatki[row['Артикул'] == nashi_fbs_ostatki['Артикул']]['Количество'].iloc[0]
        b= row['Количество']+kolvo
        return b
    else:
        return row['Количество']

alpaka_stock_template['Количество'] = alpaka_stock_template.apply(kek, axis=1)
alpaka_stock_template['Имя (необязательно)'] = alpaka_df_merged['Штрих код']
alpaka_stock_template['Заполнение обязательных ячеек'] = 'Заполнены'
alpaka_stock_template['Цена с НДС'] = alpaka_df_merged['Цена с НДС']
alpaka_stock_template['Название склада'] = 'ФБС Боровляны ООО (1020001420895000)'
alpaka_stock_template.to_excel('result/alpaka.xlsx', index=False)

spk_prefixed = [filename for filename in os.listdir('./prices') if filename.startswith("прайс акана-ориджен")]
spk_df = pd.read_excel('prices/' + spk_prefixed[0])
spk_df = spk_df.drop(spk_df.columns[[1, 3, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 18, 19]], axis=1)
spk_df.columns = spk_df.loc[11].values.flatten().tolist()
spk_df = spk_df.drop([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], axis=0)
spk_df = spk_df[spk_df['Штрихкод'].notna()]
spk_df['Штрихкод'] = spk_df['Штрихкод'].apply(lambda x: x.replace('шк', ''))
spk_df.rename(columns={'Штрихкод': 'Штрих код'}, inplace=True)
spk_df.columns = spk_df.columns.fillna('Цена с НДС')

spk_seller_products = seller_products[seller_products['Артикул'].str.startswith('ВВП')]
spk_df_merged = spk_df.merge(seller_products, on='Штрих код', how='left')
spk_df_new = spk_df_merged[spk_df_merged['Артикул_y'].isna()]
spk_df_merged = spk_seller_products.merge(spk_df, on='Штрих код', how='left')
spk_df_new.to_excel('new kartochki/spk.xlsx', index=False)

def lol2(x):
    if pd.isnull(x):
        return 0
    else:
        return random.randint(500, 1000)


spk_df_merged['Остаток'] = spk_df_merged.apply(lambda row: lol2(row['Номенклатура']), axis=1)

spk_stop_list = pd.read_excel('stop list/spk.xlsx', converters={'Штрих код': str})
spk_stop_list = spk_stop_list[spk_stop_list['Продаем(да,нет)'] == 'нет']
spk_df_merged = spk_df_merged[~spk_df_merged['Штрих код'].isin(spk_stop_list['Штрих код'])]
spk_df_merged['Остаток'] = spk_df_merged['Остаток']*(~spk_df_merged['Штрих код'].isin(spk_stop_list['Штрих код'])).astype(int)

spk_stock_template = pd.read_excel('stock-update-template.xlsx', sheet_name='Остатки на складе')
spk_stock_template['Артикул'] = spk_df_merged['Артикул_x']
spk_stock_template['Количество'] = spk_df_merged['Остаток']
spk_stock_template['Количество'] = spk_stock_template.apply(kek, axis=1)
spk_stock_template['Имя (необязательно)'] = spk_df_merged['Штрих код']
spk_stock_template['Заполнение обязательных ячеек'] = 'Заполнены'
spk_stock_template['Цена с НДС'] = spk_df_merged['Цена с НДС']
spk_stock_template['Название склада'] = 'ФБС Боровляны ООО (1020001420895000)'
spk_stock_template.to_excel('result/spk.xlsx', index=False)

trbt_prefixed = [filename for filename in os.listdir('./prices') if re.search('Прайс[0-9]{4}', filename)]
trbt_df = pd.read_excel('prices/' + trbt_prefixed[0])
trbt_df = trbt_df.drop(trbt_df.columns[[0, 1, 2, 4, 5, 6, 7, 10, 12, 13]], axis=1)
trbt_df.columns = trbt_df.loc[10].values.flatten().tolist()
trbt_df = trbt_df.drop([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13], axis=0)
trbt_df = trbt_df[~trbt_df['Штрихкод'].isna()]
trbt_df.rename(columns={'Штрихкод': 'Штрих код'}, inplace=True)
trbt_df['Штрих код'] = trbt_df['Штрих код'].astype("string")

trbt_seller_products = seller_products[seller_products['Артикул'].str.startswith('ТРБТ')]
trbt_df_merged = trbt_df.merge(seller_products, on='Штрих код', how='left')
trbt_df_new = trbt_df_merged[trbt_df_merged['Артикул'].isna()]
trbt_df_new.to_excel('new kartochki/trbt.xlsx', index=False)
trbt_df_merged = trbt_seller_products.merge(trbt_df, on='Штрих код',how='left')

trbt_stop_list = pd.read_excel('stop list/trbt.xlsx', converters={'Штрих код': str})
trbt_stop_list = trbt_stop_list[trbt_stop_list['Продаем(да,нет)'] == 'нет']

trbt_df_merged['Свободный остаток'] = trbt_df_merged['Свободный остаток'].apply(lambda x: lol2(x))
trbt_df_merged = trbt_df_merged[~trbt_df_merged['Штрих код'].isin(trbt_stop_list['Штрих код'])]
trbt_df_merged['Свободный остаток'] = trbt_df_merged['Свободный остаток']*(~trbt_df_merged['Штрих код'].isin(trbt_stop_list['Штрих код'])).astype(int)


trbt_stock_template = pd.read_excel('stock-update-template.xlsx', sheet_name='Остатки на складе')
trbt_stock_template['Артикул'] = trbt_df_merged['Артикул']
trbt_stock_template['Количество'] = trbt_df_merged['Свободный остаток']
trbt_stock_template['Количество'] = trbt_stock_template.apply(kek, axis=1)
trbt_stock_template['Имя (необязательно)'] = trbt_df_merged['Штрих код']
trbt_stock_template['Заполнение обязательных ячеек'] = 'Заполнены'
trbt_stock_template['Название склада'] = 'ФБС Боровляны ООО (1020001420895000)'
trbt_stock_template['Цена с НДС'] = trbt_df_merged['Цена с НДС']
trbt_stock_template.to_excel('result/trbt.xlsx', index=False)

zoom_prefixed = [filename for filename in os.listdir('./prices') if re.search('Прайс [0-9]{2}.[0-9]{2}. ОПТ', filename)]
zoom_df = pd.read_excel('prices/' + zoom_prefixed[0])
zoom_df = zoom_df.drop(zoom_df.columns[[0, 4, 5, 6, 7, 9]], axis=1)
zoom_df.columns = zoom_df.loc[7].values.flatten().tolist()
zoom_df = zoom_df.drop([0, 1, 2, 3, 4, 5, 6, 7], axis=0)
zoom_df['Основной штрихкод'] = zoom_df['Основной штрихкод'].shift(periods=[-1])
zoom_df['ЗМ c НДС'] = zoom_df['ЗМ c НДС'].shift(periods=[-1])
zoom_df = zoom_df.drop_duplicates(subset=['Основной штрихкод'])
zoom_df = zoom_df[zoom_df['Основной штрихкод'].notna()]
zoom_df['Основной штрихкод'] = zoom_df['Основной штрихкод'].astype(str)
zoom_df = zoom_df[zoom_df['Основной штрихкод'].str.contains(pat="[0-9]{13}", regex=True, na=True)]
zoom_df.rename(columns={'Основной штрихкод': 'Штрих код'}, inplace=True)
zoom_df['Штрих код'] = zoom_df['Штрих код'].astype("string")

zoom_df_merged = zoom_df.merge(seller_products, on='Штрих код', how='left')
zoom_df_new = zoom_df_merged[zoom_df_merged['Артикул'].isna()]
zoom_df_new.to_excel('new kartochki/zoom.xlsx', index=False)
zoom_seller_products = seller_products[seller_products['Артикул'].str.startswith('ЗООМ')]
zoom_df_merged = zoom_seller_products.merge(zoom_df, on='Штрих код', how='left')

zoom_stop_list = pd.read_excel('stop list/zoom.xlsx', converters={'Штрих код': str})
zoom_stop_list = zoom_stop_list[zoom_stop_list['Продаем(да,нет)'] == 'нет']

zoom_df_merged['Остаток_x'] = (zoom_df_merged['Ценовая группа/ Номенклатура/ Характеристика номенклатуры']
                               .apply(lambda x: lol2(x)))
zoom_df_merged['Остаток_x'] = zoom_df_merged['Остаток_x']*(~zoom_df_merged['Штрих код'].isin(zoom_stop_list['Штрих код'])).astype(int)


zoom_stock_template = pd.read_excel('stock-update-template.xlsx', sheet_name='Остатки на складе')
zoom_stock_template['Артикул'] = zoom_df_merged['Артикул']
zoom_stock_template['Количество'] = zoom_df_merged['Остаток_x']
zoom_stock_template['Количество'] = zoom_stock_template.apply(kek, axis=1)
zoom_stock_template['Имя (необязательно)'] = zoom_df_merged['Штрих код']
zoom_stock_template['Заполнение обязательных ячеек'] = 'Заполнены'
zoom_stock_template['Название склада'] = 'ФБС Боровляны ООО (1020001420895000)'
zoom_stock_template['Цена с НДС'] = zoom_df_merged['ЗМ c НДС']
zoom_stock_template.to_excel('result/zoom.xlsx', index=False)


tian_prefixed = [filename for filename in os.listdir('./prices') if filename.startswith('ООО ТИАН')]
tian_df = pd.read_excel('prices/' + tian_prefixed[0])
tian_df = tian_df.drop(tian_df.columns[[0, 2, 3, 5, 7, 8, 9, 10]], axis=1)
tian_df.columns = tian_df.loc[1].values.flatten().tolist()
tian_df = tian_df.drop([0, 1, 2], axis=0)
tian_df = tian_df[~tian_df['Штрих-код'].isna()]
tian_df.rename(columns={'Штрих-код': 'Штрих код'}, inplace=True)
tian_df['Штрих код'] = tian_df['Штрих код'].astype("string")
tian_df = tian_df[tian_df['Наименование товаров'].str.contains('AMBROSIA')]


tian_seller_products = seller_products[seller_products['Артикул'].str.startswith('ТИАН')]
tian_df_merged = tian_df.merge(tian_seller_products, on='Штрих код', how='left')
tian_df_new = tian_df_merged[tian_df_merged['Артикул'].isna()]
tian_df_new.to_excel('new kartochki/tian.xlsx', index=False)
tian_df_merged = tian_seller_products.merge(tian_df, on='Штрих код',how='left')

tian_stop_list = pd.read_excel('stop list/tian.xlsx', converters={'Штрих код': str})
tian_stop_list = tian_stop_list[tian_stop_list['Продаем(да,нет)'] == 'нет']
tian_df_merged = tian_df_merged[~tian_df_merged['Штрих код'].isin(tian_stop_list['Штрих код'])]

tian_stock_template = pd.read_excel('stock-update-template.xlsx', sheet_name='Остатки на складе')
tian_stock_template['Артикул'] = tian_df_merged['Артикул']
tian_stock_template['Количество'] = tian_df_merged['Остаток'].apply(lambda x: lol2(x))
tian_stock_template['Количество'] = tian_stock_template.apply(kek, axis=1)
tian_stock_template['Имя (необязательно)'] = tian_df_merged['Штрих код']
tian_stock_template['Заполнение обязательных ячеек'] = 'Заполнены'
tian_stock_template['Название склада'] = 'ФБС Боровляны ООО (1020001420895000)'
tian_stock_template['Цена с НДС'] = tian_df_merged['Цена  с НДС 20%']
tian_stock_template.to_excel('result/tian.xlsx', index=False)


