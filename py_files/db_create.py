import sqlite3

from bs4 import BeautifulSoup
from pymystem3 import Mystem
import nltk
import os
import glob


def get_db_connect(db_name):
  conn = sqlite3.connect(db_name)
  cur = conn.cursor()

  cur.execute("CREATE TABLE IF NOT EXISTS text"
              " (text_id INTEGER PRIMARY KEY AUTOINCREMENT, text_info TEXT UNIQUE);")

  cur.execute("CREATE TABLE IF NOT EXISTS sentence"
              " (sent_id INTEGER PRIMARY KEY AUTOINCREMENT, text_id INTEGER, sent_text TEXT);")

  cur.execute("CREATE TABLE IF NOT EXISTS onegramm"
              " (sent_id INTEGER, word_index INTEGER, word_id INTEGER, word_ind, word_form TEXT"
              ", CONSTRAINT primarykey_sent_word PRIMARY KEY (sent_id,word_index,word_id));")

  cur.execute("CREATE TABLE IF NOT EXISTS bigramm"
              " (sent_id INTEGER, word_index INTEGER, word1_id INTEGER, word1_form TEXT, word2_id INTEGER, word2_form TEXT"
              ", CONSTRAINT primarykey_sent_word1_word2 PRIMARY KEY (sent_id,word_index,word1_id,word2_id));")

  cur.execute("CREATE TABLE IF NOT EXISTS trigramm"
              " (sent_id INTEGER, word_index INTEGER, word1_id INTEGER, word1_form TEXT, word2_id INTEGER, word2_form TEXT, word3_id INTEGER, word3_form TEXT"
              ", CONSTRAINT primarykey_sent_word1_word2_word3 PRIMARY KEY (sent_id,word_index,word1_id,word2_id,word3_id));")

  cur.execute("CREATE TABLE IF NOT EXISTS word"
              " (word_id INTEGER PRIMARY KEY AUTOINCREMENT, lemma TEXT, pos_tag TEXT"
              ", CONSTRAINT lemma_pos_tag UNIQUE (lemma,pos_tag));")

  conn.commit()

  return conn


def db_create(db_conn: sqlite3.Connection):
  dir_path = os.getcwd() + "/texts000"

  mystem = Mystem()

  for filename in glob.glob(os.path.join(dir_path, "*.shtml")):
    with open(os.path.join(dir_path, filename), 'r', encoding="windows-1251") as fhtml:
      print("processing {filik}".format(filik=filename))
      db_cur = db_conn.cursor()

      contents = fhtml.read()
      soup = BeautifulSoup(contents, 'html.parser')

      text_info = soup.find("title").string
      db_cur.execute(
        "INSERT INTO text (text_info) VALUES(?) ON CONFLICT(text_info) DO NOTHING RETURNING text_id;", (text_info, ))
      text_id = db_cur.fetchone()
      if text_id is None:
        text_id = db_cur.execute("SELECT text_id FROM text WHERE text_info=?", (text_info, )).fetchone()
      text_id = text_id[0]

      all_dd_text = soup.dd.text

      cleaned_dd_text = all_dd_text.replace("\n", '')
      cleaned_dd_text = cleaned_dd_text.replace("\xa0", '')

      sentence_list = nltk.sent_tokenize(cleaned_dd_text)

      for sentence in sentence_list:
        sent_text = sentence
        db_cur.execute("INSERT INTO sentence (text_id,sent_text) VALUES(?,?) RETURNING sent_id;", (text_id, sent_text))
        sent_id = db_cur.fetchone()[0]

        ma_list = mystem.analyze(sentence)
        word_index = 0
        prev_words = [None, None]
        for w_analyse in ma_list:
          word_form = w_analyse.get('text', '').lower()
          analyze_list = w_analyse.get('analysis', [])
          if len(analyze_list) > 1:
            pass

          if len(analyze_list) > 0:
            for analyze_dict in analyze_list:
              lemma = analyze_dict.get('lex', '#EMPTY#')
              gr = analyze_dict.get('gr', '')
              pos_tag = (gr.split("=")[0].split(",")[0]) if len(gr) > 0 else ''

              print(text_id, sent_id, sent_text, word_form, lemma, pos_tag)

              db_cur.execute("INSERT INTO word (lemma,pos_tag) VALUES(?,?) ON CONFLICT(lemma,pos_tag) DO NOTHING RETURNING word_id;", (lemma, pos_tag))
              word_id = db_cur.fetchone()
              if word_id is None:
                word_id = db_cur.execute("SELECT word_id FROM word WHERE lemma=? AND pos_tag = ?", (lemma, pos_tag)).fetchone()
              word_id = word_id[0]

              db_cur.execute("INSERT OR IGNORE INTO onegramm (sent_id,word_index,word_id,word_form) VALUES(?,?,?,?);", (sent_id, word_index, word_id, word_form))

              if word_index > 0:
                word1_id, word1_form = prev_words[1]
                db_cur.execute("INSERT OR IGNORE INTO bigramm (sent_id,word_index,word1_id,word1_form,word2_id,word2_form)"
                               " VALUES(?,?,?,?,?,?);", (sent_id, word_index-1, word1_id, word1_form, word_id, word_form))

              if word_index > 1:
                word1_id, word1_form = prev_words[0]
                word2_id, word2_form = prev_words[1]
                db_cur.execute("INSERT OR IGNORE INTO trigramm (sent_id,word_index,word1_id,word1_form,word2_id,word2_form,word3_id,word3_form)"
                               " VALUES(?,?,?,?,?,?,?,?);", (sent_id, word_index-2, word1_id, word1_form, word2_id, word2_form, word_id, word_form))

            word_index += 1
            prev_words.append(tuple([word_id, word_form]))
            prev_words.pop(0)

  db_conn.commit()


def db_main():
  db_name = "f_corpus.sqlite"
  db_conn = get_db_connect(db_name)
  db_create(db_conn)

  db_conn.close()


db_main()
