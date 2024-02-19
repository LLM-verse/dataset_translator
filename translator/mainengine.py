import math
import re
import sys
sys.path.insert(0, r'./')
from copy import deepcopy

import threading
import warnings

from typing import List, Dict, Union
from tqdm.auto import tqdm

from concurrent.futures import ThreadPoolExecutor

from .utils import timeit

class TranslateThread():

    def __init__(self, 
                    all_fields = None,
                    target_fields = None,
                    enable_sub_task_thread: bool = True,  # Enable splitting a large list into sublist if a list of one example is too large to process
                                                       # This argument go with max_list_length_per_thread
                    max_example_per_thread: int = 400,  # How many examples, each thread can contain
                    large_chunks_threshold: int = 20000,  # Maximum number of examples that will be distributed evenly across threads, any examples exceed this threshold will be process in queue
                    max_list_length_per_thread: int = 3,  # Maximum number of strings contain in a list in a single thread.
                                            # if larger, split the list into sub-list and process in parallel
                    translator = None,
                    source_lang: str = "en",
                    target_lang: str = "te",
                    fail_translation_code: str="P1OP1_F"  # Fail code for *expected* fail translation and can be removed
                                                        # post-translation
                ):

        self.translator = translator
        self.fail_translation_code = fail_translation_code
        self.source_lang = source_lang
        self.target_lang = target_lang
        
        self.target_config = all_fields
        self.target_fields = target_fields
        
        assert max_example_per_thread < large_chunks_threshold, \
                " Large chunks threshold can't be smaller than max_example per thread!"

        self.max_example_per_thread = max_example_per_thread
        self.large_chunks_threshold = large_chunks_threshold

        self.enable_sub_task_thread = enable_sub_task_thread
        if self.enable_sub_task_thread:
                self.max_list_length_per_thread = max_list_length_per_thread

        self.converted_data_translated = None
        
    @property
    def get_translator(self):
        return deepcopy(self.translator)()
    
    @staticmethod
    def split_list(input_list: List[str], max_sub_length: int) -> List[list]:
        return [input_list[x:x + max_sub_length] for x in range(0, len(input_list), max_sub_length)]


    def __translate_per_key(self, example: Dict, translator=None, progress_idx: int = 0) -> Dict:
        '''
        This function loop through each key of one example and send to __translate_texts if the value of the key is
        under a certain threshold. If exceeded, then send to __sublist_multithread_translate
        '''
        keys = self.target_config
        for key in keys:
            if example[key] == "":
                continue
            if key in self.target_fields:
                if isinstance(example[key], str):
                    if len(example[key]) > 15000:
                        warnings.warn("Example " + str(example["qas_id"]) + " have field len larger than 15000")
                        example[key] = self.__split_and_translate_large_text(example[key], translator)
                    else:
                        example[key] = self.__translate_texts(src_texts=[example[key]], translator=translator)[0]
                elif isinstance(example[key], list):
                    for idx, data in enumerate(example[key]):
                        if len(data) > 15000:
                            warnings.warn("Example " + str(example["qas_id"]) + " have field len larger than 15000")
                            example[key][idx] = self.__split_and_translate_large_text(data, translator)
                    example[key] = self.__translate_texts(src_texts=example[key], translator=translator)
        return example

    def __split_and_translate_large_text(self, text: str, translator):
        '''
        This function splits a long string into smaller chunks by sentences and translates each chunk separately.
        '''
        # Define a regular expression pattern to split text into sentences
        sentence_pattern = r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s'

        # Split the text into sentences using the pattern
        sentences = re.split(sentence_pattern, text)

        # Initialize variables for chunk creation
        chunk_size = 15000
        current_chunk = ''
        chunks = []

        # Construct chunks by adding sentences until reaching the chunk size
        for sentence in sentences:
            if len(current_chunk) + len(sentence) + 2 <= chunk_size:  # +2 for " ."
                if current_chunk:
                    current_chunk += ". " + sentence
                else:
                    current_chunk = sentence
            else:
                chunks.append(current_chunk)
                current_chunk = sentence

        # Add the remaining chunk if any
        if current_chunk:
            chunks.append(current_chunk)

        # Translate each chunk separately
        translated_chunks = self.__translate_texts(src_texts=chunks, translator=translator)

        # Join translated chunks into a single string with " . " between them
        return ". ".join(translated_chunks)


    def __sublist_multithread_translate(self,
                                       list_str: List[str],
                                       progress_idx: int = 0,
                                       field_name: str=None # The field name (key name) of one example that exceed a certain threshold and needed to be split and translate in parallel
                                       ) -> List[str]:
        '''
        This function split a large list into sub-list and translate it in parallel, orders are maintained when merge all
        sub-lists, this is useful when order are necessary (e.g Dialogs example)
        '''

        translated_list_data = []
        num_threads = len(list_str) / self.max_list_length_per_thread
        sub_str_lists = self.split_list(list_str, max_sub_length=self.max_list_length_per_thread)
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            finished_task = 0
            lock = threading.RLock()

            def callback_sub_list_done(future):
                nonlocal translated_list_data
                nonlocal finished_task
                nonlocal lock
                if not future.exception():
                    with lock:
                        # This need to be .append to keep the list structure
                        # Since this deal with sub-list and needed to be merged later
                        translated_list_data.append(future.result())
                        finished_task += 1
                else:
                    tqdm.write(f"Sub task of chunk {progress_idx} with field {field_name} failed with the following error: {future.exception()}."
                               f"Restarting thread when others finished...")
                pass

            for idx, list_chunk in enumerate(sub_str_lists):
                # Assign each thread with a new Translator instance
                future_chunk = executor.submit(self.__translate_texts,
                                               src_texts=list_chunk,
                                               translator=self.get_translator,
                                               sub_list_idx=idx)
                future_chunk.add_done_callback(callback_sub_list_done)
                future_dict = {
                    "future": future_chunk,
                    "idx": idx
                }
                futures.append(future_dict)

            # Wait for all threads to complete
            while finished_task < len(futures):
                for future_dict in futures:
                    # If exception occurs in one of the thread, restart the thread with its specific chunk
                    if future_dict['future'].exception():
                        tqdm.write(
                            f"Thread {future_dict['idx']} failed, restarting thread with chunk {future_dict['idx']}")
                        backup_future_chunk = executor.submit(self.__translate_texts,
                                                              src_texts=sub_str_lists[future_dict['idx']],
                                                              translator=self.get_translator,
                                                              sub_list_idx=future_dict['idx'])
                        backup_future_chunk.add_done_callback(callback_sub_list_done)
                        backup_future_dict = {"future": backup_future_chunk,
                                              "idx": future_dict['idx']}
                        futures[future_dict['idx']] = backup_future_dict
                        continue

            # Sorting the list of dictionaries based on the 'key' value
            translated_list_data = sorted(translated_list_data, key=lambda x: x['key'])
            # Extracting values after sorting
            translated_list_data = [item['text_list'] for item in translated_list_data]

            def flatten_list(nested_list):
                '''
                Turn a list from [[], [], []] -> []
                '''

                flattened_list = []
                for item in nested_list:
                    if isinstance(item, list):
                        flattened_list.extend(flatten_list(item))
                    else:
                        flattened_list.append(item)
                return flattened_list

            translated_list_data = flatten_list(translated_list_data)

            return translated_list_data

    def __translate_texts(self,
                          src_texts: Union[List[str], str],
                          translator = None,
                          sub_list_idx: int=None, # sub_list_idx is for pass through of index information and can be merge later by __sublist_multithread_translate
                          ) -> Union[List[str], str, Dict[List[str], int]]:
        '''
        Actual place where translation take place
        '''

        # assert self.do_translate, "Please enable translate via self.do_translate"
        # This if is for multithread Translator instance
        translator_instance = deepcopy(self.translator)() if not translator else translator

        target_texts = translator_instance.translate(src_texts,
                                                     src=self.source_lang,
                                                     dest=self.target_lang,
                                                     fail_translation_code=self.fail_translation_code)

        return {'text_list': target_texts, 'key': sub_list_idx} if sub_list_idx is not None else target_texts

    def translate_converted(self,
                            converted_data = None, # The converted data that need to be translated
                            en_data: List[str] = None,
                            desc: str = None,
                            translator = None,
                            large_chunk: List[str] = None) -> Union[None, List[str]]:
        '''
        This function support translation in multithread for large dataset
        (Does not maintain order for the final dataset)
        '''

        assert converted_data is not None or en_data is not None or large_chunk is not None, \
            "No data to translate, please provide converted_data or en_data or large_chunk" 

        if not en_data and not large_chunk:
            converted_data = converted_data
        elif not en_data:
            converted_data = large_chunk
        else:
            converted_data = en_data

        translated_data = []

        # Split large data into large chunks, recursive feed to the same function
        if len(converted_data) > self.large_chunks_threshold and large_chunk is None:
            num_large_chunks = len(converted_data) / self.large_chunks_threshold
            large_chunks = self.split_list(converted_data, max_sub_length=self.large_chunks_threshold)
            tqdm.write(
                f"Data is way too large, spliting data into {num_large_chunks} large chunk for sequential translation")

            for idx, large_chunk in enumerate(tqdm(large_chunks, desc=f"Translating large chunk ", colour="red")):
                tqdm.write(f"Processing large chunk No: {idx}")
                self.translate_converted(large_chunk=large_chunk)
            return None

        # Split large chunk into large example, recursive feed to the same function via multithread
        if len(converted_data) > self.max_example_per_thread and en_data is None:
            num_threads = len(converted_data) / self.max_example_per_thread
            chunks = self.split_list(converted_data, max_sub_length=self.max_example_per_thread)
            tqdm.write(f"Data too large, splitting data into {num_threads} chunk, each chunk is {len(chunks[0])}"
                       f" Processing with multithread...")

            # Progress bar
            desc = "Translating total converted large chunk data" if large_chunk else "Translating total converted data"
            progress_bar = tqdm(total=math.ceil(num_threads), desc=desc, position=math.ceil(num_threads)+1)

            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = []
                finished_task = 0
                # https://stackoverflow.com/questions/22885775/what-is-the-difference-between-lock-and-rlock#22885810
                lock = threading.RLock()

                def callback_done(future):
                    nonlocal translated_data
                    nonlocal finished_task
                    nonlocal progress_bar
                    nonlocal lock
                    if not future.exception():
                        with lock:
                            # This need to be += or .extend to shallow flatten the list structure
                            translated_data += future.result()
                            finished_task += 1
                            progress_bar.update(1)
                    else:
                        tqdm.write(f"Task failed with the following error: {future.exception()}."
                                   f" Restarting thread when others finished")
                        pass

                for idx, chunk in enumerate(chunks):
                    # Assign each thread with a new Translator instance
                    future_chunk = executor.submit(self.translate_converted,
                                                   en_data=chunk,
                                                   desc=f"chunk {idx}",
                                                   translator=self.get_translator)
                    future_chunk.add_done_callback(callback_done)
                    future_dict = {"future": future_chunk,
                                   "idx": idx}
                    futures.append(future_dict)

                # Wait for all threads to complete
                while finished_task < len(futures):
                    for future_dict in futures:
                        # If exception occurs in one of the thread, restart the thread with its specific chunk
                        if future_dict['future'].exception():
                            tqdm.write(
                                f"Thread {future_dict['idx']} failed, restarting thread with chunk {future_dict['idx']}")
                            backup_future_chunk = executor.submit(self.translate_converted,
                                                                  en_data=chunks[future_dict['idx']],
                                                                  desc=f"Backup chunk {future_dict['idx']}",
                                                                  translator=self.get_translator)
                            backup_future_chunk.add_done_callback(callback_done)
                            backup_future_dict = {"future": backup_future_chunk,
                                                  "idx": future_dict['idx']}
                            futures[future_dict['idx']] = backup_future_dict
                            continue

            if large_chunk:
                if not self.converted_data_translated:
                    self.converted_data_translated = translated_data
                else:
                    self.converted_data_translated += translated_data
                return None

            self.converted_data_translated = translated_data
            return None

        progress_bar_desc = "Translating converted data" if not desc else f"Translating converted data {desc}"
        for example in tqdm(converted_data, desc=progress_bar_desc, colour="#add8e6"):
            translated_data_example = self.__translate_per_key(example,
                                                               translator,
                                                               progress_idx=int(re.findall(r'\d+', desc)[0]) if desc and re.findall(r'\d+', desc) else 0)
            translated_data.append(translated_data_example)
        if en_data: return translated_data
        if large_chunk:
            # Assuming that the previous large chunk process already create self.converted_data_translated
            # This cover the case where last large chunk only contain a single thread
            self.converted_data_translated += translated_data
        else:
            self.converted_data_translated = translated_data





    # def __translate_per_key(self, example: Dict, translator = None, progress_idx: int = 0) -> Dict:
    #     '''
    #     This function loop through each key of one example and send to __translate_texts if the value of the key is
    #     under a certain threshold. If exceeded, then send to __sublist_multithread_translate
    #     '''
    #     # print(example)
    #     keys = self.target_config
    #     for key in keys:
    #         if key in self.target_fields:
    #             type = "str" if isinstance(example[key], str) else "list"
    #             if example[key] == "":
    #                 continue
    #             if type == "list":
    #                 for data in example[key]:
    #                     if len(data) > 15000:
    #                         warnings.warn("Example " + str(example["qas_id"]) + " have field len larger than 15000")
    #                         example[key].append(data[:15000])
    #             else:
    #                 if len(example[key]) > 15000:
    #                     warnings.warn("Example " + str(example["qas_id"]) + " have field len larger than 15000")
    #                     example[key] = example[key][:15000]

    #             if self.enable_sub_task_thread:
    #                 average_length_sub_task_criteria = False
    #                 if type == "list" and len(example[key]) > 2:
    #                     average_length = sum(len(lst) for lst in example[key]) / len(example[key])
    #                     if average_length > 1600: average_length_sub_task_criteria = True
    #                 if type == "list" and average_length_sub_task_criteria and len(example[key]) >= self.max_list_length_per_thread:
    #                     # tqdm.write(f"\nSplitting {key} field which contain {len(example[key])} items on chunk {progress_idx}\n")
    #                     del translator
    #                     example[key] = self.__sublist_multithread_translate(example[key],
    #                                                                         progress_idx,
    #                                                                         key)
    #                 else:
    #                     example[key] = self.__translate_texts(src_texts=example[key], translator=translator)
    #             else:
    #                 example[key] = self.__translate_texts(src_texts=example[key], translator=translator)

    #     return example