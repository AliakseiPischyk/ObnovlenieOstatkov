import os
import re
from prices_processors import *

try_download_seller_products_from_ozon()

seller_products_start_tag = 'seller_products'
seller_products_prefixed = [filename for filename in os.listdir('.') if filename.startswith(seller_products_start_tag)]
if not is_one_file(seller_products_prefixed, 'Товары.цсв ' + seller_products_start_tag):
    print('Пока не почините товары.цсв, ничего работать не будет!')
    exit(1)

seller_products = parse_seller_products(seller_products_prefixed[0])
our_fbs_stock = parse_our_fbs_stock()


print(' ')
print(' ')
alpaka_start_tag = 'price_mi'
alpaka_word = 'АЛЬПАКА'
alpaka_prefixed = [filename for filename in os.listdir('./prices') if filename.startswith(alpaka_start_tag)]

if is_one_file(alpaka_prefixed, alpaka_word + ' ' + alpaka_start_tag):
    process_alpaka(alpaka_prefixed, alpaka_word, seller_products, our_fbs_stock)
else:
    info_price_not_proccessed(alpaka_word)


print(' ')
print(' ')
spk_start_tag = 'прайс акана-ориджен'
spk_word = 'СПК ВВП'
spk_prefixed = [filename for filename in os.listdir('./prices') if filename.startswith(spk_start_tag)]

if is_one_file(spk_prefixed, spk_word + ' ' + spk_start_tag):
    process_spk(spk_prefixed, spk_word, seller_products, our_fbs_stock)
else:
    info_price_not_proccessed(spk_word)


print(' ')
print(' ')
trbt_start_tag = 'Прайс[0-9]{4}'
trbt_word = 'ТРБТ'
trbt_prefixed = [filename for filename in os.listdir('./prices') if re.search(trbt_start_tag, filename)]

if is_one_file(trbt_prefixed, trbt_word + ' ' + trbt_start_tag):
    process_trbt(trbt_prefixed, trbt_word, seller_products, our_fbs_stock)
else:
    info_price_not_proccessed(trbt_word)


print(' ')
print(' ')
zoom_start_tag = 'Прайс [0-9]{2}.[0-9]{2}. ОПТ'
zoom_word = 'ЗООМ'
zoom_prefixed = [filename for filename in os.listdir('./prices') if re.search(zoom_start_tag, filename)]

if is_one_file(zoom_prefixed, zoom_word + ' ' + zoom_start_tag):
    process_zoom(zoom_prefixed, zoom_word, seller_products, our_fbs_stock)
else:
    info_price_not_proccessed(zoom_word)


print(' ')
print(' ')
tian_start_tag = 'ООО ТИАН'
tian_word = 'ТИАН'
tian_prefixed = [filename for filename in os.listdir('./prices') if filename.startswith(tian_start_tag)]

if is_one_file(tian_prefixed, tian_word + ' ' + tian_start_tag):
    process_tian(tian_prefixed, tian_word, seller_products, our_fbs_stock)
else:
    info_price_not_proccessed(tian_word)


print(' ')
print(' ')
anmd_start_tag = 'ANIMONDA'
anmd_word = 'АНИМОНДА'
anmd_prefixed = [filename for filename in os.listdir('./prices') if filename.startswith(anmd_start_tag)]

if is_one_file(anmd_prefixed, anmd_start_tag):
    process_anmd(anmd_prefixed, anmd_word, seller_products, our_fbs_stock)
else:
    info_price_not_proccessed(anmd_word)
