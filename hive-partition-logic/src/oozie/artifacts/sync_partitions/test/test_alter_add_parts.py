import unittest
from modules.AddPartitionsDeltas import GenerateALterAddPartitions

class MyTestCase(unittest.TestCase):

    def test_generate_alter_add_parts(self):
        query_result = "qtweets_raw\tcriticalstatusevent\tdatekey=20201014\n" \
                       "qtweets_raw\tcriticaladdevent\tdatekey=20201014\n" \
                       "amp_raw\trighsholder\tdatekey=20201014"
        part_fmt = "%Y%m%d"
        prev_db_val = None
        subset_result = []
        rows = query_result.split('\n')
        expected_result_qtweets = "ALTER TABLE qtweets_raw.criticalstatusevent ADD IF NOT EXISTS PARTITION (datekey='20201015');\n" \
                                  "ALTER TABLE qtweets_raw.criticaladdevent ADD IF NOT EXISTS PARTITION (datekey='20201015');\n"
        expected_result_amp_raw = "ALTER TABLE amp_raw.righsholder ADD IF NOT EXISTS PARTITION (datekey='20201015');\n"
        row_counter = 0
        generate_alter_add_obj = GenerateALterAddPartitions()
        for row_idx in range(0, len(rows)):
            row_tuple = tuple(map(lambda x: x.strip(), rows[row_idx].split('\t')))
            database = generate_alter_add_obj.desearlize_row(row_tuple)[0]
            if prev_db_val is None:
                prev_db_val = database
            if prev_db_val != database or row_counter == len(rows) - 1:
                db_alter_statements = generate_alter_add_obj.generate_time_delta_partitions(subset_result, part_fmt)
                self.assertEqual(db_alter_statements, expected_result_qtweets)
                prev_db_val = database
                subset_result = []
            subset_result.append(row_tuple)
            if row_counter == len(rows) - 1:
                db_alter_statements = generate_alter_add_obj.generate_time_delta_partitions(subset_result, part_fmt)
                self.assertEqual(db_alter_statements, expected_result_amp_raw)
            row_counter += 1

    def test_negative_alter_add_parts(self):

        query_result = "qtweets_raw\tcriticalstatusevent\tdatekey=2020-09-22\n" \
                       "qtweets_raw\tcriticaladdevent\tdatekey=2020-09-22\n" \
                       "amp_raw\trighsholder\tdatekey=2020-09-22"
        part_fmt = "%Y%m%d"
        prev_db_val = None
        subset_result = []
        rows = query_result.split('\n')
        row_counter = 0
        generate_alter_add_obj = GenerateALterAddPartitions()
        for row_idx in range(0, len(rows)):
            row_tuple = tuple(map(lambda x: x.strip(), rows[row_idx].split('\t')))
            database = generate_alter_add_obj.desearlize_row(row_tuple)[0]
            if prev_db_val is None:
                prev_db_val = database
            if prev_db_val != database or row_counter == len(rows) - 1:
                #db_alter_statements = generate_alter_add_obj.generate_time_delta_partitions(subset_result, part_fmt)
                #self.assertEqual(db_alter_statements, expected_result_qtweets)
                self.assertRaises(ValueError, generate_alter_add_obj.generate_time_delta_partitions(subset_result, part_fmt))
                prev_db_val = database
                subset_result = []
            subset_result.append(row_tuple)
            if row_counter == len(rows) - 1:
                #db_alter_statements = generate_alter_add_obj.generate_time_delta_partitions(subset_result, part_fmt)
                #self.assertEqual(db_alter_statements, expected_result_amp_raw)
                self.assertRaises(ValueError,generate_alter_add_obj.generate_time_delta_partitions(subset_result, part_fmt))
                #self.assertRaises(TypeError)
            row_counter += 1


if __name__ == '__main__':
    unittest.main()
