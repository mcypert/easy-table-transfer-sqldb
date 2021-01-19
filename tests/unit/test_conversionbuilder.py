class ConversionTestCaseBuilder:

    def __init__(self, test_name, test_case):
        self.test_name = test_name
        for k, v in test_case.items():
            setattr(self, k, [v] if type(v) is not list and k != 'test_name' else v)
        self.build_test_case()

    def number_of_test(self):
        max_len = 1
        for k, v in {k: v for k, v in self.__dict__.items() if k != 'test_name'}.items():
            if len(v) > max_len:
                max_len = len(v)
        return max_len

    def build_test_case(self):
        max_length = self.number_of_test()
        for k, v in {k: v for k, v in self.__dict__.items() if k != 'test_name'}.items():
            if len(v) != max_length:
                self.__dict__.update({k: v * max_length})

    def build_character_test(self):
        test_cases = []
        test_name = self.__dict__.get('test_name')
        data_type = self.__dict__.get("data_type")
        char_size = self.__dict__.get("char_size")
        charset = self.__dict__.get("charset")
        test_data_type = self.__dict__.get("test_data_type")
        test_char_size = self.__dict__.get("test_char_size")
        test_charset = self.__dict__.get("test_charset")

        for dt, cs, ct, tdt, tcs, tct in zip(data_type, char_size, charset, test_data_type, test_char_size, test_charset):
            column = [(test_name, {'data_type': dt, 'char_size': cs, 'num_precision': None, 'num_scale': None, 'date_precision': None, 'charset': ct}, tdt, tcs, tct)]
            test_cases.append(column)

        return test_cases

    def build_numeric_test(self):
        test_cases = []
        test_name = self.__dict__.get('test_name')
        data_type = self.__dict__.get("data_type")
        num_precision = self.__dict__.get("num_precision")
        num_scale = self.__dict__.get("num_scale")
        test_data_type = self.__dict__.get("test_data_type")
        test_num_precision = self.__dict__.get("test_num_precision")
        test_num_scale = self.__dict__.get("test_num_scale")

        for dt, np, ns, tdt, tnp, tns in zip(data_type, num_precision, num_scale, test_data_type, test_num_precision, test_num_scale):
            column = [(test_name, {'data_type': dt, 'char_size': None, 'num_precision': np, 'num_scale': ns, 'date_precision': None, 'charset': None}, tdt, tnp, tns)]
            test_cases.append(column)

        return test_cases

    def build_datetime_test(self):
        test_cases = []
        test_name = self.__dict__.get('test_name')
        data_type = self.__dict__.get("data_type")
        date_precision = self.__dict__.get("date_precision")
        test_data_type = self.__dict__.get("test_data_type")
        test_date_precision = self.__dict__.get("test_date_precision")

        for dt, dp, tdt, tdp in zip(data_type, date_precision, test_data_type, test_date_precision):
            column = [(test_name, {'data_type': dt, 'char_size': None, 'num_precision': None, 'num_scale': None, 'date_precision': dp, 'charset': None},  tdt, tdp)]
            test_cases.append(column)

        return test_cases

    def build_binary_test(self):
        pass

    def __repr__(self):
        return f'{self.__class__.__name__}()'

def test_case_list(test_style, test_dict):
    test_list = []

    for k, v in test_dict.items():
        test_case_object = ConversionTestCaseBuilder(k, v)
        test_cases = getattr(test_case_object, test_style)()
        test_list.append(test_cases)

    return [y for x in test_list for y in x]