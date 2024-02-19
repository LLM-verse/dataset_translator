from .mainengine import TranslateThread
from .providers import Provider, GoogleProvider
from typing import List, Dict, Union
from .utils import timeit, have_internet
from httpcore._exceptions import ConnectTimeout
from tqdm.auto import tqdm
from datasets import Dataset
from .filters import have_code, have_re_code
from copy import deepcopy

if not have_internet(timeout=5):
    raise ConnectTimeout("Please provide internet connection as this script require external api calls")


class TranslateModule:
    def __init__(self, provider = GoogleProvider):
        self.provider = provider
        
        self.data = None

        self.data = None

        self.all_fields = None
        self.target_fields = None
        self.no_translated_code = False # If True, will not translate data that contains code
        self.err_idx = []
        self.err_idx2 = []
        self.fail_translation_code : str="P1OP1_F"

    def read(self, dataset_split):
        
        self.all_fields = dataset_split.column_names

        data_converted = []
        qas_id = 0
        for data in tqdm(dataset_split, desc=f"Reading data"):
            data_dict = {}

            for f in self.all_fields:
                data_dict[f] = data[f]

            data_dict["qas_id"] = qas_id
            qas_id += 1
            data_converted.append(data_dict)

        self.data = data_converted

        print(f"Total data read: {len(self.data)}")
        print(f"Fields: {self.all_fields}")

    def convert(self,
        target_fields: List[str],
        source_lang: str = "en",
        target_lang: str = "te",

        enable_sub_task_thread: bool = True,
        no_translated_code: bool = False,
        max_example_per_thread = 400,
        large_chunks_threshold = 20_000,
        max_list_length_per_thread = 3,):

        self.target_fields = target_fields

        self.pre_translate_validate()

        thread = TranslateThread(
            all_fields = self.all_fields,
            target_fields = self.target_fields,
            source_lang = source_lang,
            target_lang = target_lang,
            enable_sub_task_thread = enable_sub_task_thread,
            max_example_per_thread = max_example_per_thread,
            large_chunks_threshold = large_chunks_threshold,
            max_list_length_per_thread = max_list_length_per_thread,
            translator = self.provider,)
        thread.translate_converted(converted_data = self.data)
        self.data = thread.converted_data_translated

        print(f"Total data translated: {len(self.data)}")

        self.post_translate_validate()


    @timeit
    def pre_translate_validate(self) -> None:
        validated_translate_data = []
        code_datapoints = []
        for idx, example in enumerate(tqdm(self.data, desc="Validating data for translation:")):
            for key in self.target_fields:
                if self.no_translated_code:
                    contain_code, score, found_elements = have_code(example[key])
                    if contain_code:
                        code_datapoints.append(example["qas_id"])
                        break
                    elif key == self.target_fields[-1]:
                        validated_translate_data.append(example)
                else:
                    if key == self.target_fields[-1]: validated_translate_data.append(example)

        print(f"\nTotal data left after filtering for translation: {len(validated_translate_data)}\n")
        self.data = validated_translate_data
        self.err_idx.extend(code_datapoints)

    @timeit
    def post_translate_validate(self) -> None:
        post_validated_translate_data = []
        # Note: This validates will override the original self.converted_data_translated
        for idx, example in enumerate(tqdm(self.data, desc="Validating data after translation:")):
            for key in self.target_fields:
                if have_re_code(example[key], code=self.fail_translation_code):
                    self.err_idx2.append(example["qas_id"])
                    break
                elif key == self.target_fields[-1]:
                    post_validated_translate_data.append(example)

        print(f"\nTotal data left after filtering fail translation: {len(post_validated_translate_data)}\n")
        self.data = post_validated_translate_data

    def get_hf_data(self):
        return Dataset.from_list(self.data)
        