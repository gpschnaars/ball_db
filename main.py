"""
entry script
"""

import src.game_log.pipeline
import src.player_box_scores.pipeline
import src.play_by_play.pipeline

def main():

    src.game_log.pipeline.run()
    src.player_box_scores.pipeline.run()
    src.play_by_play.pipeline.run()




if __name__ == '__main__':

    main()
