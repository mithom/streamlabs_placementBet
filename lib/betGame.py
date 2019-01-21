import random
from threading import Lock
from datetime import datetime, timedelta
from functools import wraps
import json
import time

import os

import clr

clr.AddReference("IronPython.SQLite.dll")
import sqlite3

Parent = None
random = random.WichmannHill()


def connect(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            with args[0].get_connection() as conn:
                # not using the constraints right now, would only slow things down
                # conn.execute("""PRAGMA foreign_keys = ON""")
                return f(*args, conn=conn, **kwargs)
        finally:
            if 'conn' in locals():
                # noinspection PyUnboundLocalVariable
                conn.close()
            args[0].db_lock.release()

    return wrapper


def send_stream_message(f):
    @wraps(f)
    def sender(*args, **kwargs):
        value = f(*args, **kwargs)
        if value is not None:
            Parent.SendStreamMessage(args[0].format_message(*value))

    return sender


# noinspection PyUnboundLocalVariable
class StreamSession(object):
    db_lock = Lock()
    redeem_lock = Lock()
    better_lock = Lock()
    redeemers = {}
    betters = []
    redeem_timer = None
    better_timer = None

    def __init__(self, script_settings, script_name, db_directory, overlay_directory):
        self.scriptSettings = script_settings
        self.script_name = script_name
        self.db_directory = db_directory
        self.overlay_directory = overlay_directory

        self.current_game = None
        self.session = None

        # Prepare everything
        self.prepare_database_and_session()

    def get_connection(self):
        if self.db_lock.acquire():
            return sqlite3.connect(os.path.join(self.db_directory, "database.db"), detect_types=sqlite3.PARSE_DECLTYPES)
        else:
            Parent.Log(self.script_name, 'could not acquire db lock in time (5s)')

    @connect
    def prepare_database_and_session(self, conn):
        Redeemed.create_table_if_not_exists(conn)
        Session.create_table_if_not_exists(conn)
        Game.create_database_if_not_exists(conn)
        Bet.create_table_if_not_exists(conn)
        Score.create_table_if_not_exists(conn)

        if Parent.IsLive():
            self.session = self.get_session(conn)

    @staticmethod
    def get_session(conn):
        session = Session.find_last_session(conn)
        if session is None or \
                (session.session_end is not None and session.session_end + timedelta(minutes=45) < datetime.now()):
            return Session.create(conn)
        return session

    def reset_db(self):
        self.current_game = None
        self.db_lock.acquire()
        os.remove(os.path.join(self.db_directory, "database.db"))
        self.db_lock.release()
        Parent.Log(self.script_name, 'reset successful')

    @connect
    def update(self, conn):
        if Parent.IsLive:
            if self.session is None:
                self.session = self.get_session(conn)
            else:
                now = datetime.now()
                self.session.session_end = now
                self.session.save(conn)
        elif self.session is not None:
            self.session = None

    @send_stream_message
    @connect
    def tick(self, conn):
        self.better_lock.acquire()
        try:
            if self.better_timer is not None and time.time() > self.better_timer:
                string = self.create_bet_response()
                self.better_timer = None
                self.betters = []
                return string,
        finally:
            self.better_lock.release()
        self.redeem_lock.acquire()
        try:
            if self.redeem_timer is not None and time.time() > self.redeem_timer:
                string = self.create_redeem_response()
                self.redeem_timer = None
                self.redeemers = {}
                return string,
        finally:
            self.redeem_lock.release()

    def commands(self):
        return [{
            self.scriptSettings.start_command: self.start,
            self.scriptSettings.stop_command: self.stop,
            self.scriptSettings.redeem_command: self.redeem,
        }, {
            self.scriptSettings.got_command: self.got,
        }, {
            self.scriptSettings.bet_command: self.bet,
        }]

    @send_stream_message
    @connect
    def start(self, user_id, username, conn):
        print "test"
        if Parent.HasPermission(user_id, self.scriptSettings.start_permission,
                                self.scriptSettings.start_permission_info):
            if self.current_game is None or self.current_game.status == "DONE":
                self.current_game = Game.create(conn)
                return self.scriptSettings.start_betting, self.current_game.nb
            else:
                return "there is still a game in progress",

    @send_stream_message
    @connect
    def stop(self, user_id, username, conn):
        if Parent.HasPermission(user_id, self.scriptSettings.start_permission,
                                self.scriptSettings.start_permission_info):
            if self.current_game is not None and self.current_game.status == "OPEN":
                self.current_game.status = "CLOSED"
                self.current_game.conn = conn
                self.current_game.save()
                return self.scriptSettings.end_betting, self.current_game.nb
            else:
                return "there is no game in progress to close",

    @send_stream_message
    @connect
    def redeem(self, user_id, username, conn):
        if self.session is None:
            return self.scriptSettings.offline_redeem, username
        if Redeemed.can_redeem(self.session.id_, user_id, conn):
            if Parent.HasPermission(user_id, 'Subscriber', ''):
                amount = self.scriptSettings.sub_redeem
            elif self.is_follower(username):
                amount = self.scriptSettings.follow_redeem
            else:
                return self.scriptSettings.others_message, username, self.scriptSettings.follow_redeem, \
                       self.scriptSettings.sub_redeem
            Redeemed.redeem(self.session.id_, user_id, conn)
            Parent.AddPoints(user_id, username, amount)
            return self.add_redeemed(username, amount)
        return self.scriptSettings.already_redeemed, username

    @send_stream_message
    @connect
    def got(self, user_id, username, place, conn):
        if Parent.HasPermission(user_id, self.scriptSettings.start_permission,
                                self.scriptSettings.start_permission_info):
            if self.current_game is not None and self.current_game.status == "CLOSED":
                self.current_game.status = "DONE"
                self.current_game.conn = conn
                self.current_game.save()
                total_points = self.current_game.total_amount()
                winners = self.current_game.winners(place)
                total_won = reduce(lambda x, y: x + y.amount, winners, 0)
                won_points = {x.user_id: int(round(total_points * (x.amount * 1.0 / total_won))) for x in winners}
                Parent.AddPointsAllAsync(won_points, lambda x: 0)
                for winner, points in won_points.iteritems():
                    score = Score.find_by_user_id(winner, conn)
                    if score is None:
                        score = Score.create(winner, conn)
                    score.score += points
                    score.save()
                self.update_scoreboards(conn, won_points)
                return self.scriptSettings.processing_game,
            else:
                return "there is no game or it must still be closed.",

    @send_stream_message
    @connect
    def bet(self, user_id, username, place, amount, conn):
        if self.current_game is not None and self.current_game.status == 'OPEN':
            try:
                place = int(place)
                amount = int(amount)
            except ValueError:
                return
            if amount > 0 and place > 0 and Parent.RemovePoints(user_id, username, amount):
                try:
                    Bet.create(self.current_game, user_id, place, amount, conn)
                    return self.add_better(username)
                except sqlite3.IntegrityError:
                    return self.scriptSettings.already_betted, username, self.current_game.nb
            return self.scriptSettings.not_enough_points_msg, username
        else:
            return self.scriptSettings.currently_no_bet_msg, username

    # ---------------------------------------
    #   auxiliary functions
    # ---------------------------------------
    def format_message(self, msg, *args, **kwargs):
        if self.scriptSettings.add_me and not kwargs.get('whisper', False):
            msg = "/me " + msg
        return msg.format(*args, **kwargs)

    def add_redeemed(self, username, amount):
        self.redeem_lock.acquire()
        try:
            if self.redeem_timer is None:
                self.redeem_timer = time.time()+5
            self.redeemers[username] = amount
            if reduce(lambda x, y: x + len(y[0]) + len(str(y[1])) + 5, self.redeemers.iteritems(), 19) >= 500:
                self.redeem_timer = time.time()+5
                del self.redeemers[username]
                string = self.create_redeem_response()
                self.redeemers = {username: amount}
                return string,
        finally:
            self.redeem_lock.release()

    def create_redeem_response(self):
        return self.scriptSettings.batch_redeem_msg + ', '.join(
            map(lambda x: "@{0}: {1}".format(x[0], x[1]), self.redeemers.iteritems()))

    def add_better(self, username):
        self.redeem_lock.acquire()
        try:
            if self.better_timer is None:
                self.better_timer = time.time()+5
            self.betters.append(username)
            if reduce(lambda x, y: x + len(y) + 3, self.betters, 17) >= 500:
                self.better_timer = time.time()+5
                del self.betters[-1]
                string = self.create_bet_response()
                self.betters = [username]
                return string,
        finally:
            self.redeem_lock.release()

    def create_bet_response(self):
        return self.scriptSettings.batch_bet_msg + " @" + ', @'.join(self.betters)

    @staticmethod
    def is_follower(username):
        json_data = json.loads(Parent.GetRequest(
            "https://api.ocgineer.com/twitch/followage/{0}/{1}".format(Parent.GetChannelName(), username), {}))
        if json_data["status"] == 200:
            return True
        return False

    def update_scoreboards(self, conn, won_points):
        with open(os.path.join(self.overlay_directory, 'top15game.txt'), 'w') as f:
            sorted_list = sorted(won_points.iteritems(), key=lambda tup: tup[1], reverse=True)
            if len(sorted_list) > 15:
                sorted_list = sorted_list[0:15]
            f.write('{0}\n'.format(self.scriptSettings.results_title))
            f.writelines(["{0}: {1}\n".format(Parent.GetDisplayName(pair[0]), pair[1]) for pair in sorted_list])

        with open(os.path.join(self.overlay_directory, 'top5game.txt'), 'w') as f:
            if len(sorted_list) > 5:
                sorted_list = sorted_list[0:5]
            f.write('{0}\n'.format(self.scriptSettings.results_title))
            f.writelines(["{0}: {1}\n".format(Parent.GetDisplayName(pair[0]), pair[1]) for pair in sorted_list])

        with open(os.path.join(self.overlay_directory, 'top15.txt'), 'w') as f:
            sorted_list = Score.find_all_order_by_amount(15, conn)
            f.write('{0}\n'.format(self.scriptSettings.scoreboard_title))
            f.writelines(
                ["{0}: {1}\n".format(Parent.GetDisplayName(score.user_id), score.score) for score in sorted_list])

        with open(os.path.join(self.overlay_directory, 'top5.txt'), 'w') as f:
            if len(sorted_list) > 5:
                sorted_list = sorted_list[0:5]
            f.write('{0}\n'.format(self.scriptSettings.scoreboard_title))
            f.writelines(
                ["{0}: {1}\n".format(Parent.GetDisplayName(score.user_id), score.score) for score in sorted_list])


# ---------------------------------------
#   classes representing database tables
# ---------------------------------------
class Bet(object):
    def __init__(self, id_, game, user_id, place, amount, conn):
        self.id_ = id_
        if type(game) is Game:
            self.game_id = game.id_
            self._game = game
        else:
            self.game_id = game
            self._game = None
        self.user_id = user_id
        self.place = place
        self.amount = amount

        self.conn = conn

    @classmethod
    def find_all_by_game_and_place(cls, game_id, place, conn):
        cursor = conn.execute("""SELECT * FROM bets WHERE game_id = ? AND place = ?""", (game_id, place))
        return [cls(*row, conn=conn) for row in cursor]

    @classmethod
    def find_total_by_game(cls, game_id, conn):
        cursor = conn.execute("""SELECT SUM(amount) FROM bets WHERE game_id = ?""", (game_id,))
        return cursor.fetchone()[0]

    @classmethod
    def create(cls, game, user_id, place, amount, conn):
        if type(game) is Game:
            game_id = game.id_
        else:
            game_id = game
        cursor = conn.execute("""INSERT INTO bets (game_id, user_id, place, amount) VALUES (?, ?, ?, ?)""",
                              (game_id, user_id, place, amount))
        return cls(cursor.lastrowid, game, user_id, place, amount, conn)

    @staticmethod
    def create_table_if_not_exists(conn):
        conn.execute("""CREATE TABLE IF NOT EXISTS bets
                    (id         INTEGER     PRIMARY KEY NOT NULL,
                    game_id     INTEGER     NOT NULL,
                    user_id     TEXT        NOT NULL,
                    place       INTEGER     NOT NULL,
                    amount      INTEGER     NOT NULL,
                    FOREIGN KEY (game_id)   REFERENCES games(id),
                    FOREIGN KEY (user_id)   REFERENCES scores(user_id),
                    CONSTRAINT user_game_place UNIQUE (user_id, game_id, place));""")


class Game(object):
    def __init__(self, id_, status, started_at, conn):
        self.id_ = id_
        self.status = status
        self.started_at = started_at

        self.conn = conn
        self._nb = None

    @property
    def nb(self):
        if self._nb is None:
            self._nb = Game.count(self.conn)
        return self._nb

    def winners(self, place):
        return Bet.find_all_by_game_and_place(self.id_, place, self.conn)

    def total_amount(self):
        return Bet.find_total_by_game(self.id_, self.conn)

    def save(self):
        self.conn.execute("""UPDATE games SET status = :status WHERE id = :id""",
                          {"status": self.status, "id": self.id_})

    @classmethod
    def create(cls, conn):
        now = datetime.now()
        cursor = conn.execute("""INSERT INTO games (status, started_at) VALUES ('OPEN', :now)""", {"now": now})
        conn.commit()
        return cls(cursor.lastrowid, 'OPEN', now, conn)

    @staticmethod
    def count(conn):
        cursor = conn.execute("""SELECT COUNT(*) FROM games""")
        return cursor.fetchone()[0]

    @staticmethod
    def create_database_if_not_exists(conn):
        conn.execute("""CREATE TABLE IF NOT EXISTS games
                    (id         INTEGER     PRIMARY KEY NOT NULL,
                    status      TEXT        NOT NULL,
                    started_at  TIMESTAMP   NOT NULL);""")


class Score(object):
    def __init__(self, user_id, score, conn):
        self.user_id = user_id
        self.score = score

        self.conn = conn

    def save(self):
        self.conn.execute("""UPDATE scores SET score = ? WHERE user_id = ?""", (self.score, self.user_id))

    @classmethod
    def create(cls, user_id, conn):
        conn.execute("""INSERT INTO scores (user_id, score) VALUES (?, 0)""", (user_id,))
        return cls(user_id, 0, conn)

    @classmethod
    def find_by_user_id(cls, user_id, conn):
        cursor = conn.execute("""SELECT * FROM scores WHERE user_id = ?""", (user_id,))
        row = cursor.fetchone()
        if row is None:
            return row
        return cls(*row, conn=conn)

    @classmethod
    def find_all_order_by_amount(cls, limit, conn):
        cursor = conn.execute("""SELECT * FROM scores ORDER BY score DESC LIMIT ?""", (limit,))
        return [cls(*row, conn=conn) for row in cursor]

    @staticmethod
    def create_table_if_not_exists(conn):
        conn.execute("""CREATE TABLE IF NOT EXISTS scores
                    (user_id    TEXT        PRIMARY KEY NOT NULL,
                    score       INTEGER     NOT NULL);""")


class Session(object):
    def __init__(self, id_, session_start, session_end, conn):
        self.id_ = id_
        self.session_start = session_start
        self.session_end = session_end

        self.conn = conn

    def save(self, conn=None):
        if conn is not None:
            self.conn = conn
        self.conn.execute("""UPDATE sessions SET session_end = ? WHERE id = ?""", (self.session_end, self.id_))

    @classmethod
    def create(cls, conn):
        now = datetime.now()
        cursor = conn.execute("""INSERT INTO sessions (session_start) VALUES (?)""", (now,))
        return cls(cursor.lastrowid, now, None, conn)

    @classmethod
    def find_last_session(cls, conn):
        cursor = conn.execute("""SELECT * FROM sessions ORDER BY session_start DESC LIMIT 1""")
        row = cursor.fetchone()
        if row is None:
            return None
        return cls(*row, conn=conn)

    @staticmethod
    def create_table_if_not_exists(conn):
        conn.execute("""CREATE TABLE IF NOT EXISTS sessions
         (id            INTEGER     PRIMARY KEY NOT NULL,
         session_start  TIMESTAMP   NOT NULL,
         session_end    TIMESTAMP);""")


class Redeemed(object):
    @staticmethod
    def redeem(session_id, user_id, conn):
        conn.execute("""INSERT INTO redeemed (user_id, session_id) VALUES (? , ?)""", (user_id, session_id))

    @staticmethod
    def can_redeem(session_id, user_id, conn):
        cursor = conn.execute("""SELECT * FROM redeemed WHERE session_id = ? AND user_id = ?""", (session_id, user_id))
        return cursor.fetchone() is None

    @staticmethod
    def create_table_if_not_exists(conn):
        conn.execute("""CREATE TABLE IF NOT EXISTS redeemed
                 (user_id       TEXT        NOT NULL,
                 session_id     INTEGER     NOT NULL,
                 PRIMARY KEY (user_id, session_id),
                 FOREIGN KEY (user_id)    REFERENCES scores(user_id),
                 FOREIGN KEY (session_id) REFERENCES sessions(id));""")
