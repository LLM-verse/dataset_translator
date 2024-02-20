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
        
        self.code_idx = []
        self.fail_idx = []
        self.fail_translation_code : str="P1OP1_F"

    def reset(self):
        self.code_idx = []
        self.fail_idx = []

    def read(self, dataset_split):
        
        self.reset()

        all_fields = dataset_split.column_names

        data_converted = []
        qas_id = 0
        for data in tqdm(dataset_split, desc=f"Reading data"):
            data_dict = {}

            for f in all_fields:
                data_dict[f] = data[f]

            data_dict["qas_id"] = qas_id
            qas_id += 1
            data_converted.append(data_dict)

        print(f"Total data read: {len(self.data)}")
        print(f"Fields: {self.all_fields}")
        return data_converted, all_fields

    def convert(self,
        dataset_split,
        target_fields: List[str],
        source_lang: str = "en",
        target_lang: str = "te",
        enable_sub_task_thread: bool = True,
        do_not_translate_code = False,
        max_example_per_thread = 400,
        large_chunks_threshold = 20_000,
        max_list_length_per_thread = 3,):

        data, all_fields = self.read(dataset_split)
        target_fields = target_fields

        data = self.pre_translate_validate(data, target_fields, do_not_translate_code)

        thread = TranslateThread(
            all_fields = all_fields,
            target_fields = target_fields,
            source_lang = source_lang,
            target_lang = target_lang,
            enable_sub_task_thread = enable_sub_task_thread,
            max_example_per_thread = max_example_per_thread,
            large_chunks_threshold = large_chunks_threshold,
            max_list_length_per_thread = max_list_length_per_thread,
            translator = self.provider,)

        thread.translate_converted(converted_data = data)
        data = thread.converted_data_translated

        print(f"Total data translated: {len(data)}")

        data = self.post_translate_validate()
        return get_hf_data(data)


    @timeit
    def pre_translate_validate(self, data, target_fields, do_not_translate_code) -> None:
        validated_translate_data = []
        for idx, example in enumerate(tqdm(data, desc="Validating data for translation:")):
            for key in target_fields:
                if do_not_translate_code:
                    contain_code, score, found_elements = have_code(example[key])
                    if contain_code:
                        self.code_idx.append(example["qas_id"])
                        break
                    elif key == target_fields[-1]:
                        validated_translate_data.append(example)
                else:
                    if key == target_fields[-1]: validated_translate_data.append(example)

        print(f"\nTotal data left after filtering for translation: {len(validated_translate_data)}\n")
        return validated_translate_data

    @timeit
    def post_translate_validate(self, data, target_fields) -> None:
        post_validated_translate_data = []
        # Note: This validates will override the original self.converted_data_translated
        for idx, example in enumerate(tqdm(data, desc="Validating data after translation:")):
            for key in target_fields:
                if have_re_code(example[key], code=self.fail_translation_code):
                    self.fail_idx.append(example["qas_id"])
                    break
                elif key == target_fields[-1]:
                    post_validated_translate_data.append(example)

        print(f"\nTotal data left after filtering fail translation: {len(post_validated_translate_data)}\n")
        return post_validated_translate_data

    def get_hf_data(self, data):
        dataset = Dataset.from_list(data)
        return dataset.sort("qas_id")
        
