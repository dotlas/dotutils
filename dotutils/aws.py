from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from typing import Optional, Union, Literal, Any

import json
import functools

import boto3


class Aws:
    def __init__(self, access_key: str = None, access_code: str = None):
        self.aws_session = None
        if access_key and access_code:
            if type(access_key) is str and type(access_code) is str:
                self.aws_session = boto3.Session(
                    aws_access_key_id=access_key, aws_secret_access_key=access_code
                )
            else:
                raise TypeError("AWS credentials provided are not supported data types")

        try:
            self.__s3_resource = (
                boto3.resource("s3")
                if not self.aws_session
                else self.aws_session.resource("s3")
            )
        except Exception as e:
            raise ValueError(
                f"Unable to instantiate AWS session - check credentials - {e}"
            )

    def s3_download(
        self,
        bucket: str,
        key_path: str = None,
        is_file: bool = True,
        response_type: Literal["raw", "json", "bytes", "str"] = "raw",
        threads: int = 200,
    ) -> Optional[Union[list, str]]:
        def downloader(response_type: str, s3_obj) -> str:
            s3_response = s3_obj.get()
            if s3_response:
                try:
                    if response_type == "raw":
                        return s3_response

                    s3_response_body = s3_response.get("Body")
                    bytes_response = s3_response_body.read()
                    if response_type == "bytes":
                        return bytes_response

                    str_response = bytes_response.decode("UTF-8")
                    if response_type == "str":
                        return str_response

                    json_response = json.loads(str_response)
                    if response_type == "json":
                        return json_response
                except:
                    return

        partial_downloader = functools.partial(downloader, response_type)

        if bucket and not is_file:
            s3_bucket_reference = self.__s3_resource.Bucket(bucket)
            list_keys = (
                s3_bucket_reference.objects.all()
                if not key_path
                else [
                    self.__s3_resource.Object(bucket, obj.key)
                    for obj in s3_bucket_reference.objects.filter(Prefix=key_path)
                ]
            )

            s3_response_list: list[Future] = []

            with ThreadPoolExecutor(max_workers=threads) as executor:
                threadpool_futures = [
                    executor.submit(partial_downloader, s3_key_thread)
                    for s3_key_thread in list_keys
                ]
                for future in as_completed(threadpool_futures):
                    if future.result():
                        s3_response_list.append(future.result())

            return s3_response_list

        elif bucket and is_file:
            s3_file_obj = self.__s3_resource.Object(bucket, key_path)
            return partial_downloader(s3_obj=s3_file_obj)

    def s3_upload(
        self,
        s3_obj: Any,
        bucket: str,
        key_path: str,
        metadata: dict = None,
        acl: Literal["public", "private"] = "private",
        storage_class: Literal[
            "STANDARD_IA", "STANDARD", "ONE_ZONE_IA"
        ] = "STANDARD_IA",
    ) -> dict:
        s3_file_obj = self.__s3_resource.Object(bucket, key_path)
        return s3_file_obj.put(
            ACL=acl, Body=s3_obj, Metadata=metadata, StorageClass=storage_class
        )
