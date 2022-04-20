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

    pointer.execute(
                    "SELECT p.id, "
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
                    " ORDER BY m.id"
                    )

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



@app.route('/v2/players/<string:id>/game_objectives/', methods=['GET'])
def v2_game_objectives(id):
    conn = establish_connection()
    pointer = conn.cursor()

    pointer.execute("SELECT pl.id, pl.nick AS player_nick, mpd.match_id, heroes.localized_name, "
                    "coalesce(game_objectives.subtype, 'NO ACTION') "
                    "from players AS pl "
                    "left JOIN matches_players_details AS mpd ON mpd.player_id = pl.id "
                    "left JOIN heroes ON heroes.id = mpd.hero_id "
                    "left JOIN game_objectives ON game_objectives.match_player_detail_id_1 = mpd.id "
                    "where pl.id = " + id +
                    " ORDER BY mpd.match_id")

    matches = []

    for row in pointer:
        if not response.contains('player_nick'):
            response['player_nick'] = row[1]

    current_match = None
    for match in matches:
        if match['match_id'] == row[2]:
            current_match = match['match_id']

    pointer.close()
    return json.dumps(player_dic)



@app.route('/v2/players/<string:id>/abilities/', methods=['GET'])
def v2_abilities(id):
    conn = establish_connection()
    pointer = conn.cursor()

    result = {}
    result['id'] = int(id)
    pointer.execute(
                    "SELECT p.id, COALESCE(p.nick, 'unknown') AS player_nick, "
                    "mpd.match_id, heroes.localized_name, "
                    "abilities.name, au.level "
                    "FROM players AS p "
                    "LEFT JOIN matches_players_details AS mpd ON mpd.player_id = p.id "
                    "LEFT JOIN heroes ON heroes.id = mpd.hero_id "
                    "LEFT JOIN ability_upgrades AS au ON au.match_player_detail_id = mpd.id "
                    "LEFT JOIN abilities ON abilities.id = au.ability_id "
                    "WHERE p.id = " + id +
                    " ORDER BY mpd.match_id, abilities.name, au.level "
                    )

    matches = []
    for row in pointer:
        if not 'player_nick' in result.keys():
            result['player_nick'] = row[1]

        current_match = None
        for match in matches:
            if match['match_id'] == row[2]:
                current_match = match
                break

        if current_match is not None:
            current_ability = None
            for ability in current_match['abilities']:
                if ability['ability_name'] == row[4]:
                    current_ability = ability
                    break

            if current_ability is not None:
                current_ability['upgrade_level'] = row[5]
                current_ability['count'] += 1

            else:
                current_ability = {}
                current_ability['ability_name'] = row[4]
                current_ability['upgrade_level'] = row[5]
                current_ability['count'] = 1
                current_match['abilities'].append(current_ability)

        else:
            current_match = {}
            current_match['match_id'] = row[2]
            current_match['hero_localized_name'] = row[3]
            matches.append(current_match)
            current_match['abilities'] = []

            ability = {}
            ability['ability_name'] = row[4]
            ability['count'] = 1
            ability['upgrade_level'] = row[5]
            current_match['abilities'].append(ability)

    result['matches'] = matches
    pointer.close()
    return json.dumps(result)



@app.route('/v3/matches/<string:match_id>/top_purchases/', methods=['GET'])
def v3_top_purchases(match_id):
    conn = establish_connection()
    pointer = conn.cursor()
    result = {}
    pointer.execute(
                    "SELECT * FROM (SELECT localized_name, hero_id, item_id, items.name,  "
                    "COUNT (items.id) AS counter, row_number() over (partition by localized_name ORDER BY count(items.id) DESC, items.name ASC) "
                    "FROM matches_players_details "
                    "JOIN heroes ON hero_id = heroes.id "
                    "JOIN purchase_logs ON match_player_detail_id = matches_players_details.id "
                    "JOIN items ON item_id = items.id "
                    "JOIN matches ON matches_players_details.match_id = matches.id "
                    "WHERE match_id = " + match_id + " AND (player_slot < 100 AND radiant_win = 'True' OR player_slot >= 100 AND radiant_win = 'False') "
                    "GROUP BY hero_id, localized_name, item_id, items.name "
                    "ORDER BY hero_id, counter DESC, items.name)AS vypis "
                    "WHERE row_number < 6 "
                    )

    heroes = []
    for row in pointer:
        current_hero = None
        for hero in heroes:
            if hero['id'] == row[1]:
                current_hero = hero

        if current_hero is None:
            current_hero = {}
            current_hero['id'] = row[1]
            current_hero['name'] = row[0]
            purchases = []
            purchase = {}
            purchase['id'] = row[2]
            purchase['name'] = row[3]
            purchase['count'] = row[4]
            purchases.append(purchase)

            current_hero['top_purchases'] = purchases
            heroes.append(current_hero)
        else:
            purchases = current_hero['top_purchases']
            purchase = {}
            purchase['id'] = row[2]
            purchase['name'] = row[3]
            purchase['count'] = row[4]
            purchases.append(purchase)

    result['heroes'] = heroes
    pointer.close()
    return json.dumps(result)


@app.route('/v3/statistics/tower_kills/', methods=['GET'])
def v3_tower_kills():
    conn = establish_connection()
    pointer = conn.cursor()

    pointer.execute(
                    "SELECT name, match_id, count from(select distinct  on (name) name , count(*) , match_id from( "
                    "SELECT match_id, hero_id, localized_name as name, time, subtype, "
                    "ROW_NUMBER() OVER (PARTITION BY match_id ORDER BY time ) -  "
                    "ROW_NUMBER() OVER (PARTITION BY match_id, hero_id ORDER BY time) AS sequence "
                    "FROM matches_players_details "
                    "JOIN heroes ON hero_id = heroes.id "
                    "JOIN game_objectives ON matches_players_details.id = match_player_detail_id_1   "
                    "WHERE subtype = 'CHAT_MESSAGE_TOWER_KILL' "
                    "GROUP BY hero_id, match_id, localized_name, time, subtype "
                    "ORDER BY match_id)as query1 "
                    "group by sequence, name, match_id "
                    "order by name, count desc)as query2 "
                    "order by count desc "
                    )

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