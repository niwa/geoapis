# -*- coding: utf-8 -*-
"""
Created on Wed Jun 30 11:11:25 2021

@author: pearsonra
"""

import unittest
import json
import pathlib

from src import GeoFabrics
#from src.GeoFabrics import processor
import geopandas

class Test1(unittest.TestCase):

    def test_add(self):
        
        file_path = pathlib.Path().cwd() / pathlib.Path("tests/test1/instruction.json")
        
        with open(file_path, 'r') as file_pointer:
            instructions = json.load(file_pointer)
            
        runner = GeoFabrics.processor.GeoFabricsGenerator(instructions)
        runner.run()
        #self.assertEqual((Number(5) + Number(6)).value, 11)


if __name__ == '__main__':
    unittest.main()