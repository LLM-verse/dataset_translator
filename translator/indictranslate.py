from .data_parser import DataParser
from tqdm.auto import tqdm

class DataTrans(DataParser):
    def __init__(self, file_path, output_path, hf_split, name, fields, target_fields, lang):

        self.hf_split = hf_split
        self.fields = fields

        super().__init__(file_path, output_path,
                         parser_name= name ,
                         target_config = fields, 
                         target_fields = target_fields,
                         do_translate=True,
                         no_translated_code=True,
                         target_lang=lang)

    def read(self) -> None:

        super(DataTrans, self).read()

        self.data_read = self.hf_split

        return None

    # Convert function must assign data that has been converted to self.converted_data
    def convert(self) -> None:
        # The convert function must call the convert function in DataParser class
        # I just want to be sure the read function has actually assigned the self.data_read
        super(DataTrans, self).convert()

        data_converted = []
        for data in tqdm(self.data_read, desc=f"Converting data"):
            data_dict = {}

            for f in self.fields:
                data_dict[f] = data[f]

            data_converted.append(data_dict)

        # Be sure to assign the final data list to self.converted_data
        self.converted_data = data_converted

        return None