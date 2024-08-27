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

alpaka_prefixed = [filename for filename in os.listdir('./prices') if filename.startswith("price_mi")]
alpaka_df = pd.read_excel('prices/' + alpaka_prefixed[0])

alpaka_df = alpaka_df.drop(alpaka_df.columns[[0, 2, 3, 4, 6, 7, 8, 10, 11, 12]], axis=1)
alpaka_df.columns = alpaka_df.loc[3].values.flatten().tolist()
alpaka_df = alpaka_df.drop([0, 1, 2, 3], axis=0)
alpaka_df = alpaka_df[alpaka_df['Штрихкод'].notna()]
alpaka_df = alpaka_df[alpaka_df['Наличие'] != 'Нет в наличии']
alpaka_df.rename(columns={'Штрихкод': 'Штрих код'}, inplace=True)

#alpaka_df['Штрих код'] = alpaka_df['Штрих код'].apply(lambda x: str(x)[:-2])
alpaka_df = alpaka_df[alpaka_df['Наименование'].str.startswith(("Advance", 'NT'))]
alpaka_df_merged = alpaka_df.merge(seller_products, on='Штрих код', how='left')
alpaka_df_new = alpaka_df_merged[alpaka_df_merged['Артикул'].isna()]
alpaka_df_new.to_excel('new kartochki/alpaka.xlsx', index=False)
alpaka_df_merged = alpaka_df.merge(seller_products, on='Штрих код')

alpaka_stop_list = pd.read_excel('stop list/alpaka.xlsx', converters={'Штрих код': str})
alpaka_stop_list = alpaka_stop_list[alpaka_stop_list['Продаем(да,нет)'] == 'нет']
alpaka_df_merged = alpaka_df_merged[~alpaka_df_merged['Штрих код'].isin(alpaka_stop_list['Штрих код'])]

alpaka_stock_template = pd.read_excel('stock-update-template.xlsx', sheet_name='Остатки на складе')
alpaka_stock_template['Артикул'] = alpaka_df_merged['Артикул']
alpaka_stock_template['Количество'] = random.sample(range(300, 300 + len(alpaka_stock_template.index)),
                                                    len(alpaka_stock_template.index))  #alpaka_df_merged['Остаток_x']
alpaka_stock_template['Заполнение обязательных ячеек'] = 'Заполнены'
alpaka_stock_template['Название склада'] = 'ФБС Боровляны ООО (1020001420895000)'
alpaka_stock_template.to_excel('result/alpaka.xlsx', index=False)

spk_prefixed = [filename for filename in os.listdir('./prices') if filename.startswith("прайс акана-ориджен")]
spk_df = pd.read_excel('prices/' + spk_prefixed[0])
spk_df = spk_df.drop(spk_df.columns[[1, 3, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 17, 18, 19]], axis=1)
spk_df.columns = spk_df.loc[11].values.flatten().tolist()
spk_df = spk_df.drop([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], axis=0)
spk_df = spk_df[spk_df['Штрихкод'].notna()]
spk_df['Штрихкод'] = spk_df['Штрихкод'].apply(lambda x: x.replace('шк', ''))
spk_df.rename(columns={'Штрихкод': 'Штрих код'}, inplace=True)

spk_df_merged = spk_df.merge(seller_products, on='Штрих код', how='left')
spk_df_new = spk_df_merged[spk_df_merged['Артикул_y'].isna()]
spk_df_new.to_excel('new kartochki/spk.xlsx', index=False)
spk_df_merged = spk_df.merge(seller_products, on='Штрих код')

spk_stop_list = pd.read_excel('stop list/spk.xlsx', converters={'Штрих код': str})
spk_stop_list = spk_stop_list[spk_stop_list['Продаем(да,нет)'] == 'нет']
spk_df_merged = spk_df_merged[~spk_df_merged['Штрих код'].isin(spk_stop_list['Штрих код'])]

spk_stock_template = pd.read_excel('stock-update-template.xlsx', sheet_name='Остатки на складе')
spk_stock_template['Артикул'] = spk_df_merged['Артикул_y']
spk_stock_template['Количество'] = random.sample(range(300, 300 + len(spk_stock_template.index)),
                                                 len(spk_stock_template.index))
spk_stock_template['Заполнение обязательных ячеек'] = 'Заполнены'
spk_stock_template['Название склада'] = 'ФБС Боровляны ООО (1020001420895000)'
spk_stock_template.to_excel('result/spk.xlsx', index=False)

trbt_prefixed = [filename for filename in os.listdir('./prices') if re.search('Прайс[0-9]{4}', filename)]
trbt_df = pd.read_excel('prices/' + trbt_prefixed[0])
trbt_df = trbt_df.drop(trbt_df.columns[[0, 1, 2, 4, 5, 6, 7, 10, 11, 12, 13]], axis=1)
trbt_df.columns = trbt_df.loc[10].values.flatten().tolist()
trbt_df = trbt_df.drop([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13], axis=0)
trbt_df = trbt_df[trbt_df['Свободный остаток'].notna()]
trbt_df.rename(columns={'Штрихкод': 'Штрих код'}, inplace=True)
trbt_df['Штрих код'] = trbt_df['Штрих код'].astype("string")

trbt_df_merged = trbt_df.merge(seller_products, on='Штрих код', how='left')
trbt_df_new = trbt_df_merged[trbt_df_merged['Артикул'].isna()]
trbt_df_new.to_excel('new kartochki/trbt.xlsx', index=False)
trbt_df_merged = trbt_df.merge(seller_products, on='Штрих код')

trbt_stop_list = pd.read_excel('stop list/trbt.xlsx', converters={'Штрих код': str})
trbt_stop_list = trbt_stop_list[trbt_stop_list['Продаем(да,нет)'] == 'нет']
trbt_df_merged = trbt_df_merged[~trbt_df_merged['Штрих код'].isin(trbt_stop_list['Штрих код'])]

trbt_stock_template = pd.read_excel('stock-update-template.xlsx', sheet_name='Остатки на складе')
trbt_stock_template['Артикул'] = trbt_df_merged['Артикул']
trbt_stock_template['Количество'] = random.sample(range(300, 300 + len(trbt_stock_template.index)),
                                                  len(trbt_stock_template.index))
trbt_stock_template['Заполнение обязательных ячеек'] = 'Заполнены'
trbt_stock_template['Название склада'] = 'ФБС Боровляны ООО (1020001420895000)'
trbt_stock_template.to_excel('result/trbt.xlsx', index=False)

zoom_prefixed = [filename for filename in os.listdir('./prices') if re.search('Прайс [0-9]{2}.[0-9]{2}. ОПТ', filename)]
zoom_df = pd.read_excel('prices/' + zoom_prefixed[0])
zoom_df = zoom_df.drop(zoom_df.columns[[0, 4, 5, 6, 7, 8, 9]], axis=1)
zoom_df.columns = zoom_df.loc[7].values.flatten().tolist()
zoom_df = zoom_df.drop([0, 1, 2, 3, 4, 5, 6, 7], axis=0)
zoom_df['Основной штрихкод']=zoom_df['Основной штрихкод'].shift(periods=[-1])
zoom_df = zoom_df.drop_duplicates(subset=['Основной штрихкод'])
zoom_df = zoom_df[zoom_df['Основной штрихкод'].notna()]
zoom_df = zoom_df[zoom_df['Основной штрихкод'].str.contains(pat="[0-9]{13}", regex=True, na=True)]
zoom_df.rename(columns={'Основной штрихкод': 'Штрих код'}, inplace=True)
zoom_df['Штрих код'] = zoom_df['Штрих код'].astype("string")

zoom_df_merged = zoom_df.merge(seller_products, on='Штрих код', how='left')
zoom_df_new = zoom_df_merged[zoom_df_merged['Артикул'].isna()]
zoom_df_new.to_excel('new kartochki/zoom.xlsx', index=False)
zoom_df_merged = zoom_df.merge(seller_products, on='Штрих код')

zoom_stop_list = pd.read_excel('stop list/zoom.xlsx', converters={'Штрих код': str})
zoom_stop_list = zoom_stop_list[zoom_stop_list['Продаем(да,нет)'] == 'нет']
zoom_df_merged = zoom_df_merged[~zoom_df_merged['Штрих код'].isin(zoom_stop_list['Штрих код'])]

zoom_stock_template = pd.read_excel('stock-update-template.xlsx', sheet_name='Остатки на складе')
zoom_stock_template['Артикул'] = zoom_df_merged['Артикул']
zoom_stock_template['Количество'] = random.sample(range(300, 300 + len(zoom_stock_template.index)),
                                                  len(zoom_stock_template.index))
zoom_stock_template['Заполнение обязательных ячеек'] = 'Заполнены'
zoom_stock_template['Название склада'] = 'ФБС Боровляны ООО (1020001420895000)'
zoom_stock_template.to_excel('result/zoom.xlsx', index=False)
