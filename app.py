from flask import Flask, render_template, request, redirect, url_for
import psycopg2 as psy
from dotenv import dotenv_values
import json
import os


from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import BigInteger, Boolean, CheckConstraint, Column, DateTime, ForeignKey, Index, Integer, Numeric, \
    SmallInteger, String, Table, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

app = Flask(__name__)
Base = declarative_base()
metadata = Base.metadata


#alchemy_env = dotenv_values("/home/en_var.env")
#app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + alchemy_env['DBUSER'] + ':' + alchemy_env['DBPASS'] + '@147.175.150.216/dota2'
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + os.getenv('DBUSER') + ':' + os.getenv('DBSPASS') + '@147.175.150.216/dota2'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class Ability(db.Model):
    __tablename__ = 'abilities'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text)


class AuthGroup(db.Model):
    __tablename__ = 'auth_group'

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('auth_group_id_seq'::regclass)"))
    name = db.Column(db.String(150), nullable=False, unique=True)


class AuthUser(db.Model):
    __tablename__ = 'auth_user'

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('auth_user_id_seq'::regclass)"))
    password = db.Column(db.String(128), nullable=False)
    last_login = db.Column(db.DateTime(True))
    is_superuser = db.Column(db.Boolean, nullable=False)
    username = db.Column(db.String(150), nullable=False, unique=True)
    first_name = db.Column(db.String(150), nullable=False)
    last_name = db.Column(db.String(150), nullable=False)
    email = db.Column(db.String(254), nullable=False)
    is_staff = db.Column(db.Boolean, nullable=False)
    is_active = db.Column(db.Boolean, nullable=False)
    date_joined = db.Column(db.DateTime(True), nullable=False)


class ClusterRegion(db.Model):
    __tablename__ = 'cluster_regions'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text)


class DjangoContentType(db.Model):
    __tablename__ = 'django_content_type'
    __table_args__ = (
        UniqueConstraint('app_label', 'model'),
    )

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('django_content_type_id_seq'::regclass)"))
    app_label = db.Column(db.String(100), nullable=False)
    model = db.Column(db.String(100), nullable=False)


class DjangoMigration(db.Model):
    __tablename__ = 'django_migrations'

    id = db.Column(db.BigInteger, primary_key=True, server_default=text("nextval('django_migrations_id_seq'::regclass)"))
    app = db.Column(db.String(255), nullable=False)
    name = db.Column(db.String(255), nullable=False)
    applied = db.Column(db.DateTime(True), nullable=False)


class DjangoSession(db.Model):
    __tablename__ = 'django_session'

    session_key = db.Column(db.String(40), primary_key=True, index=True)
    session_data = db.Column(db.Text, nullable=False)
    expire_date = db.Column(db.DateTime(True), nullable=False, index=True)


class Hero(db.Model):
    __tablename__ = 'heroes'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text)
    localized_name = db.Column(db.Text)


class Item(db.Model):
    __tablename__ = 'items'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text)


class Patch(db.Model):
    __tablename__ = 'patches'

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('patches_id_seq'::regclass)"))
    name = db.Column(db.Text, nullable=False)
    release_date = db.Column(db.DateTime, nullable=False)


class Player(db.Model):
    __tablename__ = 'players'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text)
    nick = db.Column(db.Text)


class AuthPermission(db.Model):
    __tablename__ = 'auth_permission'
    __table_args__ = (
        UniqueConstraint('content_type_id', 'codename'),
    )

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('auth_permission_id_seq'::regclass)"))
    name = db.Column(db.String(255), nullable=False)
    content_type_id = db.Column(db.ForeignKey('django_content_type.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)
    codename = db.Column(db.String(100), nullable=False)

    content_type = db.relationship('DjangoContentType')


class AuthUserGroup(db.Model):
    __tablename__ = 'auth_user_groups'
    __table_args__ = (
        UniqueConstraint('user_id', 'group_id'),
    )

    id = db.Column(db.BigInteger, primary_key=True, server_default=text("nextval('auth_user_groups_id_seq'::regclass)"))
    user_id = db.Column(db.ForeignKey('auth_user.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)
    group_id = db.Column(db.ForeignKey('auth_group.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)

    group = db.relationship('AuthGroup')
    user = db.relationship('AuthUser')


class DjangoAdminLog(db.Model):
    __tablename__ = 'django_admin_log'
    __table_args__ = (
        CheckConstraint('action_flag >= 0'),
    )

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('django_admin_log_id_seq'::regclass)"))
    action_time = db.Column(db.DateTime(True), nullable=False)
    object_id = db.Column(db.Text)
    object_repr = db.Column(db.String(200), nullable=False)
    action_flag = db.Column(db.SmallInteger, nullable=False)
    change_message = db.Column(db.Text, nullable=False)
    content_type_id = db.Column(db.ForeignKey('django_content_type.id', deferrable=True, initially='DEFERRED'), index=True)
    user_id = db.Column(db.ForeignKey('auth_user.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)

    content_type = db.relationship('DjangoContentType')
    user = db.relationship('AuthUser')


class Match(db.Model):
    __tablename__ = 'matches'

    id = db.Column(db.Integer, primary_key=True)
    cluster_region_id = db.Column(db.ForeignKey('cluster_regions.id'))
    start_time = db.Column(db.Integer)
    duration = db.Column(db.Integer)
    tower_status_radiant = db.Column(db.Integer)
    tower_status_dire = db.Column(db.Integer)
    barracks_status_radiant = db.Column(db.Integer)
    barracks_status_dire = db.Column(db.Integer)
    first_blood_time = db.Column(db.Integer)
    game_mode = db.Column(db.Integer)
    radiant_win = db.Column(db.Boolean)
    negative_votes = db.Column(db.Integer)
    positive_votes = db.Column(db.Integer)

    cluster_region = db.relationship('ClusterRegion')


class PlayerRating(db.Model):
    __tablename__ = 'player_ratings'

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('player_ratings_id_seq'::regclass)"))
    player_id = db.Column(db.ForeignKey('players.id'))
    total_wins = db.Column(db.Integer)
    total_matches = db.Column(db.Integer)
    trueskill_mu = db.Column(db.Numeric)
    trueskill_sigma = db.Column(db.Numeric)

    player = db.relationship('Player')


class AuthGroupPermission(db.Model):
    __tablename__ = 'auth_group_permissions'
    __table_args__ = (
        UniqueConstraint('group_id', 'permission_id'),
    )

    id = db.Column(db.BigInteger, primary_key=True, server_default=text("nextval('auth_group_permissions_id_seq'::regclass)"))
    group_id = db.Column(db.ForeignKey('auth_group.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)
    permission_id = db.Column(db.ForeignKey('auth_permission.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)

    group = db.relationship('AuthGroup')
    permission = db.relationship('AuthPermission')


class AuthUserUserPermission(db.Model):
    __tablename__ = 'auth_user_user_permissions'
    __table_args__ = (
        UniqueConstraint('user_id', 'permission_id'),
    )

    id = db.Column(db.BigInteger, primary_key=True, server_default=text("nextval('auth_user_user_permissions_id_seq'::regclass)"))
    user_id = db.Column(db.ForeignKey('auth_user.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)
    permission_id = db.Column(db.ForeignKey('auth_permission.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)

    permission = db.relationship('AuthPermission')
    user = db.relationship('AuthUser')


class MatchesPlayersDetail(db.Model):
    __tablename__ = 'matches_players_details'
    __table_args__ = (
        Index('idx_match_id_player_id', 'match_id', 'player_slot', 'id'),
    )

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('matches_players_details_id_seq'::regclass)"))
    match_id = db.Column(db.ForeignKey('matches.id'))
    player_id = db.Column(db.ForeignKey('players.id'))
    hero_id = db.Column(db.ForeignKey('heroes.id'))
    player_slot = db.Column(db.Integer)
    gold = db.Column(db.Integer)
    gold_spent = db.Column(db.Integer)
    gold_per_min = db.Column(db.Integer)
    xp_per_min = db.Column(db.Integer)
    kills = db.Column(db.Integer)
    deaths = db.Column(db.Integer)
    assists = db.Column(db.Integer)
    denies = db.Column(db.Integer)
    last_hits = db.Column(db.Integer)
    stuns = db.Column(db.Integer)
    hero_damage = db.Column(db.Integer)
    hero_healing = db.Column(db.Integer)
    tower_damage = db.Column(db.Integer)
    item_id_1 = db.Column(db.ForeignKey('items.id'))
    item_id_2 = db.Column(db.ForeignKey('items.id'))
    item_id_3 = db.Column(db.ForeignKey('items.id'))
    item_id_4 = db.Column(db.ForeignKey('items.id'))
    item_id_5 = db.Column(db.ForeignKey('items.id'))
    item_id_6 = db.Column(db.ForeignKey('items.id'))
    level = db.Column(db.Integer)
    leaver_status = db.Column(db.Integer)
    xp_hero = db.Column(db.Integer)
    xp_creep = db.Column(db.Integer)
    xp_roshan = db.Column(db.Integer)
    xp_other = db.Column(db.Integer)
    gold_other = db.Column(db.Integer)
    gold_death = db.Column(db.Integer)
    gold_abandon = db.Column(db.Integer)
    gold_sell = db.Column(db.Integer)
    gold_destroying_structure = db.Column(db.Integer)
    gold_killing_heroes = db.Column(db.Integer)
    gold_killing_creeps = db.Column(db.Integer)
    gold_killing_roshan = db.Column(db.Integer)
    gold_killing_couriers = db.Column(db.Integer)

    hero = db.relationship('Hero')
    item = db.relationship('Item', primaryjoin='MatchesPlayersDetail.item_id_1 == Item.id')
    item1 = db.relationship('Item', primaryjoin='MatchesPlayersDetail.item_id_2 == Item.id')
    item2 = db.relationship('Item', primaryjoin='MatchesPlayersDetail.item_id_3 == Item.id')
    item3 = db.relationship('Item', primaryjoin='MatchesPlayersDetail.item_id_4 == Item.id')
    item4 = db.relationship('Item', primaryjoin='MatchesPlayersDetail.item_id_5 == Item.id')
    item5 = db.relationship('Item', primaryjoin='MatchesPlayersDetail.item_id_6 == Item.id')
    match = db.relationship('Match')
    player = db.relationship('Player')


class Teamfight(db.Model):
    __tablename__ = 'teamfights'
    __table_args__ = (
        Index('teamfights_match_id_start_teamfight_id_idx', 'match_id', 'start_teamfight', 'id'),
    )

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('teamfights_id_seq'::regclass)"))
    match_id = db.Column(db.ForeignKey('matches.id'))
    start_teamfight = db.Column(db.Integer)
    end_teamfight = db.Column(db.Integer)
    last_death = db.Column(db.Integer)
    deaths = db.Column(db.Integer)

    match = db.relationship('Match')


class AbilityUpgrade(db.Model):
    __tablename__ = 'ability_upgrades'

    id = db.Column(Integer, primary_key=True, server_default=text("nextval('ability_upgrades_id_seq'::regclass)"))
    ability_id = db.Column(db.ForeignKey('abilities.id'))
    match_player_detail_id = db.Column(db.ForeignKey('matches_players_details.id'))
    level = db.Column(Integer)
    time = db.Column(Integer)

    ability = db.relationship('Ability')
    match_player_detail = db.relationship('MatchesPlayersDetail')


class Chat(db.Model):
    __tablename__ = 'chats'

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('chats_id_seq'::regclass)"))
    match_player_detail_id = db.Column(db.ForeignKey('matches_players_details.id'))
    message = db.Column(db.Text)
    time = db.Column(db.Integer)
    nick = db.Column(db.Text)

    match_player_detail = relationship('MatchesPlayersDetail')


class GameObjective(db.Model):
    __tablename__ = 'game_objectives'

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('game_objectives_id_seq'::regclass)"))
    match_player_detail_id_1 = db.Column(db.ForeignKey('matches_players_details.id'))
    match_player_detail_id_2 = db.Column(db.ForeignKey('matches_players_details.id'))
    key = db.Column(db.Integer)
    subtype = db.Column(db.Text)
    team = db.Column(db.Integer)
    time = db.Column(db.Integer)
    value = db.Column(db.Integer)
    slot = db.Column(db.Integer)

    matches_players_detail = db.relationship('MatchesPlayersDetail', primaryjoin='GameObjective.match_player_detail_id_1 == MatchesPlayersDetail.id')
    matches_players_detail1 = db.relationship('MatchesPlayersDetail', primaryjoin='GameObjective.match_player_detail_id_2 == MatchesPlayersDetail.id')


class PlayerAction(db.Model):
    __tablename__ = 'player_actions'

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('player_actions_id_seq'::regclass)"))
    unit_order_none = db.Column(db.Integer)
    unit_order_move_to_position = db.Column(db.Integer)
    unit_order_move_to_target = db.Column(db.Integer)
    unit_order_attack_move = db.Column(db.Integer)
    unit_order_attack_target = db.Column(db.Integer)
    unit_order_cast_position = db.Column(db.Integer)
    unit_order_cast_target = db.Column(db.Integer)
    unit_order_cast_target_tree = db.Column(db.Integer)
    unit_order_cast_no_target = db.Column(db.Integer)
    unit_order_cast_toggle = db.Column(db.Integer)
    unit_order_hold_position = db.Column(db.Integer)
    unit_order_train_ability = db.Column(db.Integer)
    unit_order_drop_item = db.Column(db.Integer)
    unit_order_give_item = db.Column(db.Integer)
    unit_order_pickup_item = db.Column(db.Integer)
    unit_order_pickup_rune = db.Column(db.Integer)
    unit_order_purchase_item = db.Column(db.Integer)
    unit_order_sell_item = db.Column(db.Integer)
    unit_order_disassemble_item = db.Column(db.Integer)
    unit_order_move_item = db.Column(db.Integer)
    unit_order_cast_toggle_auto = db.Column(db.Integer)
    unit_order_stop = db.Column(db.Integer)
    unit_order_buyback = db.Column(db.Integer)
    unit_order_glyph = db.Column(db.Integer)
    unit_order_eject_item_from_stash = db.Column(db.Integer)
    unit_order_cast_rune = db.Column(db.Integer)
    unit_order_ping_ability = db.Column(db.Integer)
    unit_order_move_to_direction = db.Column(db.Integer)
    match_player_detail_id = db.Column(db.ForeignKey('matches_players_details.id'))

    match_player_detail = db.relationship('MatchesPlayersDetail')


class PlayerTime(db.Model):
    __tablename__ = 'player_times'

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('player_times_id_seq'::regclass)"))
    match_player_detail_id = db.Column(ForeignKey('matches_players_details.id'))
    time = db.Column(db.Integer)
    gold = db.Column(db.Integer)
    lh = db.Column(db.Integer)
    xp = db.Column(db.Integer)

    match_player_detail = db.relationship('MatchesPlayersDetail')


class PurchaseLog(db.Model):
    __tablename__ = 'purchase_logs'

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('purchase_logs_id_seq'::regclass)"))
    match_player_detail_id = db.Column(db.ForeignKey('matches_players_details.id'))
    item_id = db.Column(db.ForeignKey('items.id'))
    time = db.Column(db.Integer)

    item = db.relationship('Item')
    match_player_detail = db.relationship('MatchesPlayersDetail')


class TeamfightsPlayer(db.Model):
    __tablename__ = 'teamfights_players'

    id = db.Column(db.Integer, primary_key=True, server_default=text("nextval('teamfights_players_id_seq'::regclass)"))
    teamfight_id = db.Column(db.ForeignKey('teamfights.id'))
    match_player_detail_id = db.Column(db.ForeignKey('matches_players_details.id'))
    buyback = db.Column(db.Integer)
    damage = db.Column(db.Integer)
    deaths = db.Column(db.Integer)
    gold_delta = db.Column(db.Integer)
    xp_start = db.Column(db.Integer)
    xp_end = db.Column(db.Integer)

    match_player_detail = db.relationship('MatchesPlayersDetail')
    teamfight = db.relationship('Teamfight')



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
    result['id'] = int(match_id)
    pointer.close()
    return json.dumps(result)


@app.route('/v3/statistics/tower_kills/', methods=['GET'])
def v3_tower_kills():
    conn = establish_connection()
    pointer = conn.cursor()
    result = {}
    pointer.execute("SELECT name, match_id, count from(select distinct  on (name) name , count(*) , match_id from( "
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

    kills = []
    for row in pointer:
        kill = {}
        kill['id'] = row[1]
        kill['name'] = row[0]
        kill['tower_kill'] = row[2]
        kills.append(kill)

    result['heroes'] = kills
    pointer.close()
    return json.dumps(result)


## Zadanie 6


@app.route('/v4/patches/', methods=['GET'])
def v4_patches():
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



@app.route('/v4/players/<string:id>/game_exp/', methods=['GET'])
def v4_game_exp(id):
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



@app.route('/v4/players/<string:id>/game_objectives/', methods=['GET'])
def v4_game_objectives(id):
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



@app.route('/v4/players/<string:id>/abilities/', methods=['GET'])
def v4_abilities(id):
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



@app.route('/v4/matches/<string:match_id>/top_purchases/', methods=['GET'])
def v4_top_purchases(match_id):
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
    result['id'] = int(match_id)
    pointer.close()
    return json.dumps(result)


@app.route('/v4/statistics/tower_kills/', methods=['GET'])
def v4_tower_kills():
    conn = establish_connection()
    pointer = conn.cursor()
    result = {}
    pointer.execute("SELECT name, match_id, count from(select distinct  on (name) name , count(*) , match_id from( "
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

    kills = []
    for row in pointer:
        kill = {}
        kill['id'] = row[1]
        kill['name'] = row[0]
        kill['tower_kill'] = row[2]
        kills.append(kill)

    result['heroes'] = kills
    pointer.close()
    return json.dumps(result)

## Zadanie 6 koniec






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