'''
Test Remote HDS Parser

Created on 11 May 2015

@author: Alastair McCormack
'''
import urlparse
import os.path
import requests
import f4v
from datetime import timedelta

class HdsServerReader(object):
    
    def __init__(self, bootstrap_data):
        parser = f4v.F4VParser()
        self.parsed_bootstrap = parser.parse(bytes_input=bootstrap_data)
        
    def get_latest_frag_suffix(self):
        last_run_table = list(self.parsed_bootstrap)[-1]
        
#         for srt in last_run_table.segment_run_tables:
#             print srt
#         
#         for ft in last_run_table.fragment_tables:
#             for frt in ft.fragments:
#                 print frt

        current_time = last_run_table.current_media_time
        
        last_known_fragment = last_run_table.fragment_tables[-1].fragments[-1]
        last_known_fragment_number = last_known_fragment.first_fragment
        last_known_fragment_time = last_known_fragment.first_fragment_timestamp
        last_known_fragment_duration = last_known_fragment.fragment_duration
        last_known_fragment_duration_td = timedelta(milliseconds = last_known_fragment_duration )
        
        segment_number = last_run_table.segment_run_tables[-1].segment_run_table_entries[-1].first_segment
        
        time_difference = current_time - last_known_fragment_time
        frag_number_difference = time_difference.seconds /  last_known_fragment_duration_td.seconds
        last_fragment = last_known_fragment_number + frag_number_difference - 1
        
        filename = "Seg{segment_number}-Frag{frag_number}".format(segment_number=segment_number,
                                                                  frag_number=last_fragment)
        
        return filename