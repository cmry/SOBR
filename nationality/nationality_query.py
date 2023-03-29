from pymongo import MongoClient
from bson.json_util import dumps, loads
from tqdm import tqdm
import time
import datetime

subreddits = ['r/europe', 'r/AskEurope', 'r/EuropeanCulture', 'r/EuropeanFederalists', 'r/Eurosceptics']

client = MongoClient("localhost", 27010)             
db = client.research
db.authenticate("sergey", "topsecretpasswordforsergeysmongo")

database_month = '07-2021'

regex_dic = {
    'Germany': 'Germany|DE|Deutschland|Deutsch|Deutsche|Bavaria|North Rhine-Westphalia|Berlin|Lower Saxony|Baden-W\u00fcrttemberg|Franconia|:flag[_-]de:',
    'United Kingdom': 'United Kingdom|UK|England|British|Scotland|Wales|Northern Ireland|Great Britain|Ulster|Munster|London|British Isles|:flag[_-]gb[_-]eng:',
    'USA': 'USA|US|United States|American|Americans|America|United States of America|California|NYC|Texas|:flag[_-]us:',
    'Turkey': 'Turkey|TR|Turkish|Türkiye|T\u00fcrkiye|:flag[_-]tr:',
    'Finland': 'Finland|FI|Finnish|Suomi|:flag[_-]fi:',
    'Sweden': 'Sweden|SE|Swedish|Sverige|:flag[_-]se:',
    'Italy': 'Italy|IT|Italian|Italia|Emilia-Romagna|Lazio|Lombardy|Veneto|Abruzzo|Sardinia|Sicily|:flag[_-]it:',
    'France': 'France|FR|French|Français|Française|:flag[_-]fr:',
    'Greece': 'Greece|GR|Greek|Ελλάδα|Hellas|Pontus|:flag[_-]gr:',
    'The Netherlands': 'The Netherlands|NL|Dutch|Netherlands|Nederland|Limburg|Amsterdam|:flag[_-]nl:',
    'Poland': 'Poland|PL|Polish|Polska|Warsaw|:flag[_-]pl:',
    'Denmark': 'Denmark|DK|Danish|Danmark|Zealand|Copenhagen|Faroe Islands|:flag[_-]dk:',
    'Romania': 'Romania|RO|Romanian|Rumänien|Bucharest|:flag[_-]ro:',
    'Serbia': 'Serbia|RS|Serbian|Србија|:flag[_-]rs:',
    'Ireland': 'Ireland|IE|Irish|Éire|Leinster|:flag[_-]ie:',
    'Croatia': 'Croatia|HR|Croatian|Hrvatska|Dalmatia|:flag[_-]hr:',
    'Portugal': 'Portugal|PT|Portuguese|Portugal|:flag[_-]pt:',
    'Bulgaria': 'Bulgaria|BG|Bulgarian|България|:flag[_-]bg:',
    'Norway': 'Norway|NO|Norwegian|Norge|:flag[_-]no:',
    'Lithuania': 'Lithuania|LT|Lithuanian|Lietuva|:flag[_-]lt:',
    'Austria': 'Austria|AT|Austrian|Österreich|\u00d6sterreich|:flag[_-]at:',
    'Estonia': 'Estonia|EE|Estonian|Eesti|:flag[_-]ee:',
    'Belgium': 'Belgium|BE|Belgian|België|Flanders|Brussels|Ghent|:flag[_-]be:',
    'Spain': 'Spain|ES|Spanish|España|:flag[_-]es:',
    'Ukraine': 'Ukraine|UA|Ukrainian|Україна|Kyiv|:flag[_-]ua:',
    'Czech Republic': 'Czech Republic|CZ|Czech|Česká|Czechia|:flag[_-]cz:',
    'Hungary': 'Hungary|HU|Hungarian|Magyarország|:flag[_-]hu:',
    'Slovakia': 'Slovakia|SK|Slovak|Slovensko|:flag[_-]sk:',
    'Slovenia': 'Slovenia|SI|Slovenian|Slovenija|:flag[_-]si:',
    'Brazil': 'Brazil|BR|Brazilian|Brasil|:flag[_-]br:',
    'Norway': 'Norway|NO|Norwegian|Norge|:flag[_-]no:',
    'Canada': 'Canada|CA|Canadian|Canada|:flag[_-]ca:',
    'Russia': 'Russia|RU|Russian|Россия|St. Petersburg|:flag[_-]ru:',
    'Switzerland': 'Switzerland|CH|Swiss|Schweiz|:flag[_-]ch:',
    'Georgia': 'Georgia|GE|Georgian|საქართველო|:flag[_-]ge:',
    'North Macedonia': 'North Macedonia|MK|Macedonian|Македонија|Macedonia|:flag[_-]mk:',
    'Luxembourg': 'Luxembourg|LU|Luxembourgish|Luxembourg|:flag[_-]lu:',
    'Cyprus': 'Cyprus|CY|Cypriot|Κύπρος|:flag[_-]cy:',
    'Montenegro': 'Montenegro|ME|Montenegrin|Црна Гора|:flag[_-]me:',
    'Catalonia': 'Catalonia|CT|Catalan|Catalunya|:flag[_-]ct:',
    'Bosnia and Herzegovina': 'Bosnia and Herzegovina|BA|Bosnian|Bosna i Hercegovina|Bosnia|Herzegovina|:flag[_-]ba:',
    'New Zealand': 'New Zealand|NZ|New Zealand|New Zealand|:flag[_-]nz:',
    'Iceland': 'Iceland|IS|Icelandic|Ísland|:flag[_-]is:',
    'Israel': 'Israel|IL|Israeli|ישראל|:flag[_-]il:',
    'Albania': 'Albania|AL|Albanian|Shqipëria|:flag[_-]al:',
    'Australia': 'Australia|AU|Australian|Australia|:flag[_-]au:',
    'Malta': 'Malta|MT|Maltese|Malta|:flag[_-]mt:',
    'Mexico': 'Mexico|MX|Mexican|México|:flag[_-]mx:',
    'Kosovo': 'Kosovo|XK|Kosovar|Kosova|:flag[_-]xk:',
    'Greenland': 'Greenland|GL|Greenlandic|Kalaallit Nunaat|:flag[_-]gl:',
    'Kuwait': 'Kuwait|KW|Kuwaiti|الكويت|:flag[_-]kw:',
    'Moldova': 'Moldova|MD|Moldovan|Moldova|:flag[_-]md:',
    'Kazakhstan': 'Kazakhstan|KZ|Kazakh|Қазақстан|:flag[_-]kz:',
    'Argentina': 'Argentina|AR|Argentine|Argentina|:flag[_-]ar:',
    'Azerbaijan': 'Azerbaijan|AZ|Azerbaijani|Azərbaycan|:flag[_-]az:',
    'Cuba': 'Cuba|CU|Cuban|Cuba|:flag[_-]cu:',
    'Japan': 'Japan|JP|Japanese|日本|:flag[_-]jp:',
    'Philippines': 'Philippines|PH|Filipino|Pilipinas|:flag[_-]ph:',
    'Liechtenstein': 'Liechtenstein|LI|Liechtenstein|Liechtenstein|:flag[_-]li:',
    'Armenia': 'Armenia|AM|Armenian|Հայաստան|:flag[_-]am:',
    'Taiwan': 'Taiwan|TW|Taiwanese|台灣|:flag[_-]tw:',
    'Chili': 'Chili|CL|Chilean|Chile|:flag[_-]cl:',
    'India': 'India|IN|Indian|भारत|:flag[_-]in:',
    'Iran': 'Iran|IR|Iranian|ایران|:flag[_-]ir:',
    'Uruguay': 'Uruguay|UY|Uruguayan|Uruguay|:flag[_-]uy:',
    'Syria': 'Syria|SY|Syrian|سوريا|:flag[_-]sy:',
}

t0 = time.time()

main_db_pipeline = [
    # match only the subreddits we want
    {'$match': {'subreddit_name_prefixed': {'$in': subreddits}}},

    # create a new field with the matched regex value
    {'$addFields': {
        'matched_regex': {
            '$reduce': {
                'input': {'$objectToArray': regex_dic},
                'initialValue': [],
                'in': {
                    '$concatArrays': [
                        '$$value',
                        {'$cond': [
                            {'$regexMatch': {'input': '$author_flair_text', 'regex': '$$this.v'}},
                            ['$$this.k'],
                            []
                        ]}
                    ]
    }}}}}, 

    # Get rid of posts with no matches and posts with multiple matches
    {'$match': {'matched_regex': {'$size': 1}}},

    # Get rid of unnecessary fields
    {'$project': {'author_fullname': 1, 'author_flair_text': 1, 'subreddit_name_prefixed': 1, 'post_id': '$id', 'matched_regex': {'$first': '$matched_regex'}, '_id': 0}}, 

    # Group by author
    {'$group': {'_id': '$author_fullname', 'posts': {'$addToSet': {'post_id': '$post_id', 'flair': '$author_flair_text', 'subreddit_with_prefix': '$subreddit_name_prefixed', 'database_month': database_month}}, 'labels': {'$addToSet': '$matched_regex'}}},

    # Get rid of authors with multiple labels
    {'$match': {'labels': {'$size': 1}}},

    # Convert the labels array to a string
    {'$addFields': {'label': {'$first': '$labels'}}},

    # Get rid of unnecessary fields 
    {'$project': {'posts': 1, 'label': 1, 'author_id': '$_id', '_id': 0}},

    # Save intermediate results to a temporary collection
    {'$out': 'nationality_temp'}
]

db.july2021_all.aggregate(main_db_pipeline)

for country in regex_dic.keys():
    temp_db_pipeline = [
        # Match only the documents with the current country
        {'$match': {'label': country}},
        
        # Format the data to the desired format
        {'$addFields': {country: '$posts'}},
        {'$addFields': {'nationality': {country: '$' + country}}},
        {'$addFields': {'labels': {'nationality': '$nationality'}}},
        {'$project': {'author_id': 1, 'labels': 1, '_id': 0}},

        # Save the result to the labelled authors collection
        {'$merge': {'into': 'labelled_authors_temp',
                    'on': 'author_id',
                    'whenMatched': 'merge',
                    'whenNotMatched': 'insert'}},
    ]

    db.nationality_temp.aggregate(temp_db_pipeline)


db.nationality_temp.drop()

elapsed = str(datetime.timedelta(seconds=int(round(time.time() - t0))))
print(f'Query took: {elapsed}')