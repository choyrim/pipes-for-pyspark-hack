import os
import sys

this_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(this_dir, "src"))
sys.path.insert(0, src_dir)
