import json
import codecs

Parent = None


class Settings(object):
    # do not use multiple instances of this version of the class, as it uses class
    # variables in order to avoid being in __dict__
    settings_file = ""
    script_name = ""

    def __init__(self, settings_file, script_name):
        Settings.settings_file = settings_file
        Settings.script_name = script_name
        try:
            with codecs.open(self.settings_file, encoding="utf-8-sig", mode="r") as f:
                self.__dict__ = json.load(f, encoding="utf-8")
        except:
            # Command names
            self.start_command = "!startBet"
            self.stop_command = "!stopBet"
            self.got_command = "!got"
            self.bet_command = "!bet"
            self.redeem_command = "!redeem"

            # Config
            self.start_permission = "Caster"
            self.start_permission_info = ""
            self.update_interval = 30
            self.max_votes = 5
            self.add_me = True
            self.follow_redeem = 100
            self.sub_redeem = 300
            self.others_message = "{0}, follow to get daily {1} or sub for {2}!"
            self.scoreboard_title = "Scoreboard:"
            self.results_title = "Results:"
            self.batch_redeem_msg = "successful redeems: "
            self.currently_no_bet_msg = "{0}, you cannot currently bet."
            self.not_enough_points_msg = "{0} you don't have so many points!"
            self.already_betted = '{0}, you already placed a bet on that place for this game (#{1})'
            self.batch_bet_msg = "successful bets:"
            self.processing_game = "distributing gold..."
            self.already_redeemed = "{0}, you already redeemed today!"
            self.offline_redeem = "{0}, you can only redeem when the streamer is online"
            self.end_betting = "betting for game #{0} has closed. please wait until it ends for the results"
            self.start_betting = "viewers can now bet on game #{0} using !bet place amount"
            self.max_bet_limit_msg = "{0}, you can have a maximum of up to {1} votes"

    def reload(self, json_data):
        """ Reload settings from Chatbot user interface by given json data. """
        self.__dict__ = json.loads(json_data, encoding="utf-8")
        self.save()
        return

    def save(self):
        """ Save settings contained within to .json and .js settings files. """
        try:
            with codecs.open(self.settings_file, encoding="utf-8-sig", mode="w+") as f:
                json.dump(self.__dict__, f, encoding="utf-8", ensure_ascii=False)
            with codecs.open(self.settings_file.replace("json", "js"), encoding="utf-8-sig", mode="w+") as f:
                f.write("var settings = {0};".format(json.dumps(self.__dict__, encoding='utf-8', ensure_ascii=False)))
        except:
            Parent.Log(self.script_name, "Failed to save settings to file.")
        return
