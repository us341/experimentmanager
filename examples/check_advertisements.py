"""
This script will simply do an advertise lookup of a given key.
"""

import sys

# If this script resides outside of the directory that contains the seattlelib
# files and experimentlib.py, then you'll need to set that path here. If you
# downloaded an installer (even if you haven't installed seattle on the machine
# this script resides on), the path will be to the seattle_repy directory from
# the extracted installer. 
#PATH_TO_SEATTLE_REPY = "/path/to/seattle_repy"
#sys.path.append(PATH_TO_SEATTLE_REPY)

import experimentlib




PUBLICKEY = "65537 123456789"





def main():

  identity = experimentlib.create_identity_from_key_strings(PUBLICKEY)

  nodelocation_list = experimentlib.lookup_node_locations_by_identity(identity)

  print("Number of advertising nodes: " + str(len(nodelocation_list)))
  
  for nodelocation in nodelocation_list:
    print(nodelocation)



if __name__ == "__main__":
  main()
