# ---------------------------------------
#   Import Libraries
# ---------------------------------------
import os
import time
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))  # Point at lib folder for classes / references
import clr
clr.AddReference("IronPython.Modules.dll")

import betGame
import SettingsModule

# ---------------------------------------
#   [Required]  Script Information
# ---------------------------------------
ScriptName = "csgoBet"
Website = "https://www.twitch.tv/mi_thom"
Description = "bet on the ending place of the streamer"
Creator = "mi_thom / powerclan"
Version = "0.0.1"

# ---------------------------------------
#   Global Variables
# ---------------------------------------
m_settings_file = os.path.join(os.path.dirname(__file__), "Settings", "csgoBet_settings.json")
ScriptSettings = None
game = None
next_update = 0
next_tick = 0


# ---------------------------------------
#   main interface
# ---------------------------------------
# noinspection PyPep8Naming
def Init():
    global ScriptSettings, game, next_update
    # Insert Parent in submodules
    betGame.Parent = Parent
    SettingsModule.Parent = Parent

    #   Create Settings and db Directory
    settings_directory = os.path.join(os.path.dirname(__file__), "Settings")
    if not os.path.exists(settings_directory):
        os.makedirs(settings_directory)

    db_directory = os.path.join(os.path.dirname(__file__), "db")
    if not os.path.exists(db_directory):
        os.makedirs(db_directory)

    overlay_directory = os.path.join(os.path.dirname(__file__), "overlay")
    if not os.path.exists(overlay_directory):
        os.makedirs(overlay_directory)

    #   Load settings
    ScriptSettings = SettingsModule.Settings(m_settings_file, ScriptName)

    # Create game
    game = betGame.StreamSession(ScriptSettings, ScriptName, db_directory, overlay_directory)

    # Prepare Tick()
    next_update = time.time()


# noinspection PyPep8Naming
def ReloadSettings(jsondata):
    ScriptSettings.reload(jsondata)


# noinspection PyPep8Naming
def Unload():
    ScriptSettings.save()


# noinspection PyPep8Naming
def ScriptToggle(state):
    global next_update
    # next_update is time remaining in tick while script is toggled off.
    if state:
        next_update += time.time()
    else:
        next_update -= time.time()


# noinspection PyPep8Naming
def Tick():
    if time.time() >= next_update:
        set_next_update()
        game.update()
    if time.time() >= next_tick:
        set_next_tick()
        game.tick()


# noinspection PyPep8Naming
def Execute(data):
    if data.IsChatMessage():
        p_count = data.GetParamCount()
        command_functions = game.commands()
        if p_count <= len(command_functions):
            param0 = data.GetParam(0)
            if param0 in command_functions[p_count-1]:
                command_functions[p_count-1][param0](data.User, data.UserName, *data.Message.split()[1:])


def OpenOverlayFolder():
    os.startfile(os.path.join(os.path.dirname(__file__), "overlay"))


def StartNewSession():
    try:
        with game.get_connection() as conn:
            game.session = betGame.StreamSession.create()
    finally:
        if 'conn' in locals():
            # noinspection PyUnboundLocalVariable
            conn.close()
        game.db_lock.release()


push_time = 0
push_count = 0


def ResetDatabase():
    global push_time, push_count
    if time.time() > push_time:
        push_count = 0
        push_time = time.time() + 5
    push_count += 1
    if push_count >= 5:
        game.reset_db()
        Init()


# ---------------------------------------
#   auxiliary functions
# ---------------------------------------
def set_next_update():
    global next_update
    next_update = time.time() + ScriptSettings.update_interval

def set_next_tick():
    global next_tick
    next_tick = time.time() + 0.1
