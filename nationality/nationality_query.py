from pymongo import MongoClient
from bson.json_util import dumps, loads
from tqdm import tqdm

subreddits = ['r/europe', 'r/AskEurope', 'r/EuropeanCulture', 'r/EuropeanFederalists', 'r/Eurosceptics']

client = MongoClient("localhost", 27010)             
db = client.research
db.authenticate("sergey", "topsecretpasswordforsergeysmongo")

database_month = '07-2021'

regex_dic = {
    'Germany': 'Germany|DE|Deutschland|Deutsch|Deutsche|Bavaria|North Rhine-Westphalia|Berlin|Lower Saxony|Baden-W\u00fcrttemberg|:flag_de:',
    'United Kingdom': 'United Kingdom|UK|England|British|Scotland|Wales|Northern Ireland|Great Britain|Ulster|Munster|London|:flag_gb_eng:',
    'USA': 'USA|US|United States|American|Americans|America|United States of America|California|NYC|:flag_us:',
    'Turkey': 'Turkey|TR|Turkish|Türkiye|T\u00fcrkiye|:flag_tr:',
    'Finland': 'Finland|FI|Finnish|Suomi|:flag_fi:',
    'Sweden': 'Sweden|SE|Swedish|Sverige|:flag_se:',
    'Italy': 'Italy|IT|Italian|Italia|Emilia-Romagna|Lazio|Lombardy|Veneto|Abruzzo|Sardinia|:flag_it:',
    'France': 'France|FR|French|Français|Française|:flag_fr:',
    'Greece': 'Greece|GR|Greek|Ελλάδα|Hellas|:flag_gr:',
    'The Netherlands': 'The Netherlands|NL|Dutch|Netherlands|Nederland|Limburg|Amsterdam|:flag_nl:',
    'Poland': 'Poland|PL|Polish|Polska|Warsaw|:flag_pl:',
    'Denmark': 'Denmark|DK|Danish|Danmark|Zealand|Copenhagen|:flag_dk:',
    'Romania': 'Romania|RO|Romanian|Rumänien|Bucharest|:flag_ro:',
    'Serbia': 'Serbia|RS|Serbian|Србија|:flag_rs:',
    'Ireland': 'Ireland|IE|Irish|Éire|Leinster|:flag_ie:',
    'Croatia': 'Croatia|HR|Croatian|Hrvatska|Dalmatia|:flag_hr:',
    'Portugal': 'Portugal|PT|Portuguese|Portugal|:flag_pt:',
    'Bulgaria': 'Bulgaria|BG|Bulgarian|България|:flag_bg:',
    'Norway': 'Norway|NO|Norwegian|Norge|:flag_no:',
    'Lithuania': 'Lithuania|LT|Lithuanian|Lietuva|:flag_lt:',
    'Austria': 'Austria|AT|Austrian|Österreich|\u00d6sterreich|:flag_at:',
    'Estonia': 'Estonia|EE|Estonian|Eesti|:flag_ee:',
    'Belgium': 'Belgium|BE|Belgian|België|Flanders|Brussels|:flag_be:',
    'Spain': 'Spain|ES|Spanish|España|:flag_es:',
    'Ukraine': 'Ukraine|UA|Ukrainian|Україна|Kyiv|:flag_ua:',
    'Czech Republic': 'Czech Republic|CZ|Czech|Česká|Czechia|:flag_cz:',
    'Hungary': 'Hungary|HU|Hungarian|Magyarország|:flag_hu:',
    'Slovakia': 'Slovakia|SK|Slovak|Slovensko|:flag_sk:',
    'Slovenia': 'Slovenia|SI|Slovenian|Slovenija|:flag_si:',
    'Brazil': 'Brazil|BR|Brazilian|Brasil|:flag_br:',
    'Norway': 'Norway|NO|Norwegian|Norge|:flag_no:',
    'Canada': 'Canada|CA|Canadian|Canada|:flag_ca:',
    'Russia': 'Russia|RU|Russian|Россия|St. Petersburg|:flag_ru:',
    'Switzerland': 'Switzerland|CH|Swiss|Schweiz|:flag_ch:',
    'Georgia': 'Georgia|GE|Georgian|საქართველო|:flag_ge:',
    'North Macedonia': 'North Macedonia|MK|Macedonian|Македонија|Macedonia|:flag_mk:',
    'Luxembourg': 'Luxembourg|LU|Luxembourgish|Luxembourg|:flag_lu:',
    'Cyprus': 'Cyprus|CY|Cypriot|Κύπρος|:flag_cy:',
    'Montenegro': 'Montenegro|ME|Montenegrin|Црна Гора|:flag_me:',
    'Catalonia': 'Catalonia|CT|Catalan|Catalunya|:flag_ct:',
    'Bosnia and Herzegovina': 'Bosnia and Herzegovina|BA|Bosnian|Bosna i Hercegovina|Bosnia|Herzegovina|:flag_ba:',
    'New Zealand': 'New Zealand|NZ|New Zealand|New Zealand|:flag_nz:',
    'Iceland': 'Iceland|IS|Icelandic|Ísland|:flag_is:',
    'Israel': 'Israel|IL|Israeli|ישראל|:flag_il:',
    'Albania': 'Albania|AL|Albanian|Shqipëria|:flag_al:',
    'Australia': 'Australia|AU|Australian|Australia|:flag_au:',
    'Malta': 'Malta|MT|Maltese|Malta|:flag_mt:',
    'Mexico': 'Mexico|MX|Mexican|México|:flag_mx:',
    'Kosovo': 'Kosovo|XK|Kosovar|Kosova|:flag_xk:',
    'Greenland': 'Greenland|GL|Greenlandic|Kalaallit Nunaat|:flag_gl:',
    'Kuwait': 'Kuwait|KW|Kuwaiti|الكويت|:flag_kw:',
    'Moldova': 'Moldova|MD|Moldovan|Moldova|:flag_md:',
    'Kazakhstan': 'Kazakhstan|KZ|Kazakh|Қазақстан|:flag_kz:',
    'Argentina': 'Argentina|AR|Argentine|Argentina|:flag_ar:',
    'Azerbaijan': 'Azerbaijan|AZ|Azerbaijani|Azərbaycan|:flag_az:',
    'Cuba': 'Cuba|CU|Cuban|Cuba|:flag_cu:',
    'Japan': 'Japan|JP|Japanese|日本|:flag_jp:',
    'Philippines': 'Philippines|PH|Filipino|Pilipinas|:flag_ph:',
    'Liechtenstein': 'Liechtenstein|LI|Liechtenstein|Liechtenstein|:flag_li:',
    'Armenia': 'Armenia|AM|Armenian|Հայաստան|:flag_am:',
    'India': 'India|IN|Indian|भारत|:flag_in:',
    'China': 'China|CN|Chinese|中国|:flag_cn:',
}

for country, regex in tqdm(regex_dic.items()):

    pipeline = [
        {'$match': {'subreddit_name_prefixed': {'$in': subreddits}}},
        {'$match': {'author_flair_text': {'$regex':regex}}},
        {'$project': {'author_fullname': 1, 'author_flair_text': 1, 'subreddit_name_prefixed': 1, 'post_id': '$id', '_id': 0}}, 
        {'$group': {'_id': '$author_fullname', country: {'$addToSet': {'post_id': '$post_id', 'flair': '$author_flair_text', 'subreddit_with_prefix': '$subreddit_name_prefixed', 'database_month': database_month}}}},
        {'$addFields': {'nationality': {country: '$' + country}}},
        {'$addFields': {'labels': {'nationality': '$nationality'}}},
        {'$project': {'author_id': '$_id', 'labels': 1, '_id': 0}},
        {'$out': 'labelled_authors_temp'}
    ]

    results = list(db.july2021_all.aggregate(pipeline))
