from flask import Flask, render_template, request, redirect, url_for
import psycopg2 as psy
from dotenv import dotenv_values
import json
import os
app = Flask(__name__)


@app.route('/')
def index():
   print('Request for index page received')
   return render_template('index.html')


def win_establish_connection():
    conn = psy.connect(
       host="147.175.150.216",
       database="dota2",
       user=os.getenv('DBUSER'),
       password=os.getenv('DBSPASS'))
    return conn


def lin_establish_connection():
    var = dotenv_values("/home/en_var.env")
    conn = psy.connect(
       host="147.175.150.216",
       database="dota2",
       user=var['DBUSER'],
       password=var['DBSPASS'])
    return conn

def establish_connection():
    if os.name == "nt":     # vrati nt pre widnows a posix pre linux a mac
        conn = win_establish_connection()
    else:
        conn = lin_establish_connection()
    return conn

@app.route('/v1/health', methods=['GET'])
def v1_health():
    print('Request for index page received')
    conn = establish_connection()
    pointer = conn.cursor()

    pointer.execute("SELECT VERSION()")
    version = pointer.fetchone()

    pointer.execute("SELECT pg_database_size('dota2')/1024/1024 as dota2_db_size")
    size = pointer.fetchone()

    response = {}
    response2 = {}
    response2['pgsql'] = response
    response["dota2_db_size"] = size[0]
    response['version'] = version[0]

    final_response = json.dumps(response2)
    return final_response


@app.route('/v2/patches/', methods=['GET'])
def v2_patches():
    conn = establish_connection()
    pointer = conn.cursor()

    pointer.execute("SELECT all_paches.patch_version, all_paches.patch_start_date, all_paches.patch_end_date, matches.id AS match_id, duration "
                    "FROM matches "
                    "LEFT JOIN( "
                    "SELECT p1.name as patch_version, "
                    "extract(EPOCH from p1.release_date) as patch_start_date, "
                    "extract(EPOCH from p2.release_date) as patch_end_date "
                    "FROM patches as p1 "
                    "LEFT JOIN patches as p2 "
                    "ON p1.id = p2.id - 1 "
                    "ORDER BY p1.id "
                    ") AS all_paches "
                    "ON matches.start_time > all_paches.patch_start_date AND matches.start_time < COALESCE(all_paches.patch_end_date, 9999999999)")

    response = {}
    response['patches'] = []

    for row in pointer:
        current_patch = None
        for patch in response['patches']:
            if patch['patch_version'] == str(row[0]):
                current_patch = patch
                break

        if current_patch is not None:
            match = {}
            match['match_id'] = row[3]
            match['duration'] = row[4]
            current_patch['matches'].append(match)

        else:
            current_patch = {}
            current_patch['patch_version'] = row[0]
            current_patch['patch_start_date'] = row[1]
            current_patch['patch_end_date'] = row[2]
            current_patch['matches'] = []

            match = {}
            match['match_id'] = row[3]
            match['duration'] = row[4]
            current_patch['matches'].append(match)
            response['patches'].append(current_patch)

    return json.dumps(response)


@app.route('/v2/players/<string:id>/game_exp/', methods=['GET'])
def v2_game_exp(id):
    conn = establish_connection()
    pointer = conn.cursor()

    player_dic = {}
    player_dic['id'] = int


    pointer.execute("SELECT p.id, "
                    "COALESCE(p.nick,'unknown') AS player_nick, "
                    "match_id, "
                    "localized_name AS hero_localized_name, "
                    "ROUND(m.duration/60.0, 2) AS match_duration_minutes, "
                    "COALESCE(xp_hero, 0) + COALESCE(xp_creep,0) + COALESCE(xp_other,0) + COALESCE(xp_roshan,0) as experiences_gained, "
                    "greatest(level) as level_gained, "
                    "CASE WHEN mpd.player_slot < 5 AND m.radiant_win = 'true' OR mpd.player_slot >127 AND m.radiant_win = 'false' "
                    "THEN 'true' ELSE 'false' END AS winner "
                    "FROM matches_players_details as mpd "
                    "JOIN players as p "
                    "on p.id = mpd.player_id "
                    "JOIN heroes as h "
                    "on mpd.hero_id = h.id "
                    "JOIN matches as m "
                    "on mpd.match_id = m.id "
                    "WHERE p.id =" + id +
                    " ORDER BY m.id")

    final_response = pointer.fetchone()
    return final_response

"""

ak SQL vrati tabulku, treba pouzit

for row in pointer:
    print("Riadok: " + row[0] + " | " + row[1]...)

"""




@app.route('/hello', methods=['POST'])
def hello():
   name = request.form.get('name')

   if name:
       print('Request for hello page received with name=%s' % name)
       return render_template('hello.html', name = name)
   else:
       print('Request for hello page received with no name or blank name -- redirecting')
       return redirect(url_for('index'))


if __name__ == '__main__':
   app.run()