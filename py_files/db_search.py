import sqlite3
from pymystem3 import Mystem


def get_db_read_connect(db_name):
    conn = sqlite3.connect(db_name)
    return conn


def get_word_condition(num: int, word: str):
    mystem = Mystem()

    # Если требовалось найти определенную словоформу

    sql_where = ''

    if word.startswith('"'):  # word form condition
        word_form = word[1:-1]
        word_form = word_form.lower()
        sql_where = "form{num}='{frm}'".format(num=num, frm=word_form)

    else:  # Если требовалось найти по лемме
        lemma = mystem.lemmatize(word)[0]

        sql_where = "lem{num}='{lem}'".format(num=num, lem=lemma)

    return sql_where


def db_search_main():
    pos_tag_set = {"A", "ADV", "ADVPRO", "ANUM", "APRO", "COM", "CONJ", "INTJ", "NUM", "PART", "PR", "S", "SPRO", "V"}

    print('Для ввода словоформы напишите слово в кавычках (например, "смешной")')
    print('Для ввода леммы напишите слово без кавычек (например, смешной)')
    print('Для поиска части речи, пожалуйста, введите тэг (A, ADV, ADVPRO, ANUM, APRO, COM, CONJ, INTJ, NUM, PART, PR, S, SPRO, V) (например, V)')
    print('Для поиска леммы или словоформы определнной части речи напишите между ними + (например, "гуляю"+V)')
    print('Максимальная длина запроса 3, вводите элементы через пробел')

    query = input()


# query = 'V "мне"+SPRO S'
# query = 'V "мне"'
# query = 'V'

# Анализ введенного запроса, исходя из его длины
    token_list = query.split(' ')
    if len(token_list) == 1:
        sql_select = "SELECT t.text_id,t.text_info,s.sent_text" \
                     ",g1.word_form AS form1,w1.lemma AS lem1,w1.pos_tag AS pos1" \
                     " FROM text AS t" \
                     " INNER JOIN sentence AS s on s.text_id=t.text_id" \
                     " INNER JOIN onegramm AS g1 on g1.sent_id=s.sent_id" \
                     " INNER JOIN word AS w1 on w1.word_id=g1.word_id"
    elif len(token_list) == 2:
        sql_select = "SELECT t.text_id,t.text_info,s.sent_text" \
                     ",g2.word1_form AS form1,w1.lemma AS lem1,w1.pos_tag AS pos1" \
                     ",g2.word2_form AS form2,w2.lemma AS lem2,w2.pos_tag AS pos2" \
                     " FROM text AS t" \
                     " INNER JOIN sentence AS s on s.text_id=t.text_id" \
                     " INNER JOIN bigramm AS g2 on g2.sent_id=s.sent_id" \
                     " INNER JOIN word AS w1 on w1.word_id=g2.word1_id" \
                     " INNER JOIN word AS w2 on w2.word_id=g2.word2_id"
    elif len(token_list) == 3:
        sql_select = "SELECT t.text_id,t.text_info,s.sent_text" \
                     ",g3.word1_form AS form1,w1.lemma AS lem1,w1.pos_tag AS pos1" \
                     ",g3.word2_form AS form2,w2.lemma AS lem2,w2.pos_tag AS pos2" \
                     ",g3.word3_form AS form3,w3.lemma AS lem3,w3.pos_tag AS pos3" \
                     " FROM text AS t" \
                     " INNER JOIN sentence AS s on s.text_id=t.text_id" \
                     " INNER JOIN trigramm AS g3 on g3.sent_id=s.sent_id" \
                     " INNER JOIN word AS w1 on w1.word_id=g3.word1_id" \
                     " INNER JOIN word AS w2 on w2.word_id=g3.word2_id" \
                     " INNER JOIN word AS w3 on w3.word_id=g3.word3_id"
    else:
        print('Некорректный запрос, в следующий раз введите запрос длиной от 1 до 3')
        exit()

    token_num = 1
    sql_where_list = []  # Составление запроса в бд

    # Анализ каждого ввденного элемента запроса (ввели лемму/словоформу/часть речи)
    for token in token_list:
        if token in pos_tag_set:  # POS tag
            sql_where_list.append("pos{num}='{tkn}'".format(num=token_num, tkn=token))
        elif '+' in token:  # word+POS_tag
            word, pos_tag = tuple(token.split('+'))
            sql_where_list.append(get_word_condition(token_num, word))
            sql_where_list.append("pos{num}='{tkn}'".format(num=token_num, tkn=pos_tag))
        else:  # word form
            sql_where_list.append(get_word_condition(token_num, token))

        token_num += 1

        # Отправка запроса в бд
    db_name = "f_corpus.sqlite"
    sql_query = sql_select + ' WHERE ' + " AND ".join(sql_where_list)

    db_conn = get_db_read_connect(db_name)
    db_cur = db_conn.cursor()

    db_cur.execute(sql_query)
    row_list = db_cur.fetchall()
    db_conn.close()

    if len(row_list) == 0:
        print('Увы, ничего не нашлось:(')

    row_num = 1
    for row in row_list:
        print(row_num, ') Мета-информация: ', row[1])
        print('Предложение: ', row[2])
        row_num += 1

db_search_main()

