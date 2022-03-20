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
    pointer.close()
    return final_response


@app.route('/v2/patches/', methods=['GET'])
def v2_patches():
    conn = establish_connection()
    pointer = conn.cursor()

    pointer.execute(
                    "SELECT all_matches.match_id, round (all_matches.duration/60.0, 2) AS duration, patches.name AS patch_version, "
                    "cast (extract(epoch FROM patches.release_date) AS INT) AS patch_start_date, "
                    "cast (extract (epoch FROM next_patch.release_date) AS INT) AS patch_end_date "
                    "FROM patches "
                    "left JOIN patches AS next_patch ON patches.id = next_patch.id  - 1 "
                    "left JOIN ( "
                    "SELECT matches.id AS match_id, duration, start_time "
                    "FROM matches "
                    ") AS all_matches ON (all_matches.start_time > extract(epoch FROM patches.release_date) "
                    "and all_matches.start_time < coalesce (extract (epoch FROM next_patch.release_date) , 9999999999)) "
                    "ORDER BY patches.id"
                    )

    response = {}
    response['patches'] = []

    for row in pointer:
        current_patch = None
        for patch in response['patches']:
            if patch['patch_version'] == str(row[2]):
                current_patch = patch
                break

        if current_patch is not None:
            match = {}
            match['match_id'] = row[0]
            match['duration'] = float(row[1])
            current_patch['matches'].append(match)

        else:
            current_patch = {}
            current_patch['patch_version'] = row[2]
            current_patch['patch_start_date'] = row[3]
            current_patch['patch_end_date'] = row[4]
            current_patch['matches'] = []

            if row[0] is not None:
                match = {}
                match['match_id'] = row[0]
                match['duration'] = float(row[1])
                current_patch['matches'].append(match)

            response['patches'].append(current_patch)
    pointer.close()
    return json.dumps(response)


@app.route('/v2/players/<string:id>/game_exp/', methods=['GET'])
def v2_game_exp(id):
    conn = establish_connection()
    pointer = conn.cursor()

    pointer.execute("SELECT COALESCE(nick, 'nick') "
                    "FROM players "
                    "WHERE id = " + id)
    player_dic = {}
    player_dic['id'] = int(id)
    player_dic['player_nick'] = pointer.fetchone()[0]

    pointer.execute("SELECT p.id, "
                    "COALESCE(p.nick,'unknown') AS player_nick, "
                    "match_id, "
                    "localized_name AS hero_localized_name, "
                    "ROUND(m.duration/60.0, 2) AS match_duration_minutes, "
                    "COALESCE(xp_hero, 0) + COALESCE(xp_creep,0) + COALESCE(xp_other,0) + COALESCE(xp_roshan,0) as experiences_gained, "
                    "greatest(level) as level_gained, "
                    "CASE WHEN mpd.player_slot < 5 AND m.radiant_win = true OR mpd.player_slot >127 AND m.radiant_win = false "
                    "THEN true ELSE false END AS winner "
                    "FROM matches_players_details as mpd "
                    "JOIN players as p "
                    "on p.id = mpd.player_id "
                    "JOIN heroes as h "
                    "on mpd.hero_id = h.id "
                    "JOIN matches as m "
                    "on mpd.match_id = m.id "
                    "WHERE p.id = " + id +
                    " ORDER BY m.id")

    matches = []
    for row in pointer:
        matchess = {}
        matchess['match_id'] = row[2]
        matchess['hero_localized_name'] = row[3]
        matchess['match_duration_minutes'] = float(row[4])
        matchess['experiences_gained'] = row[5]
        matchess['level_gained'] = row[6]
        matchess['winner'] = row[7]
        matches.append(matchess)

    player_dic['matches'] = matches
    pointer.close()
    return json.dumps(player_dic)


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