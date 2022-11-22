from typing import Any, List, Optional, Union

from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from azure.core.exceptions import ResourceNotFoundError
from azure.identity.aio import ClientSecretCredential, DefaultAzureCredential
from azure.storage.blob._models import BlobProperties
from azure.storage.blob.aio import BlobClient, BlobServiceClient, ContainerClient

Credentials = Union[ClientSecretCredential, DefaultAzureCredential]


class WasbHookAsync(WasbHook):
    """
    An async hook that connects to Azure WASB to perform operations.

    :param wasb_conn_id: reference to the :ref:`wasb connection <howto/connection:wasb>`
    :param public_read: whether an anonymous public read access should be used. default is False
    """

    def __init__(
        self,
        wasb_conn_id: str = "wasb_default",
        public_read: bool = False,
    ) -> None:
        self.conn_id = wasb_conn_id
        self.public_read = public_read
        self.blob_service_client: BlobServiceClient = self.get_conn()

    def get_conn(self) -> BlobServiceClient:
        """Returns the async BlobServiceClient object."""
        conn = self.get_connection(self.conn_id)
        extra = conn.extra_dejson or {}

        if self.public_read:
            # Here we use anonymous public read
            # more info
            # https://docs.microsoft.com/en-us/azure/storage/blobs/storage-manage-access-to-resources
            return BlobServiceClient(account_url=conn.host, **extra)

        connection_string = extra.pop("connection_string", extra.pop("extra__wasb__connection_string", None))
        if connection_string:
            # connection_string auth takes priority
            return BlobServiceClient.from_connection_string(connection_string, **extra)

        shared_access_key = extra.pop("shared_access_key", extra.pop("extra__wasb__shared_access_key", None))
        if shared_access_key:
            # using shared access key
            return BlobServiceClient(account_url=conn.host, credential=shared_access_key, **extra)

        tenant = extra.pop("tenant_id", extra.pop("extra__wasb__tenant_id", None))
        if tenant:
            # use Active Directory auth
            app_id = conn.login
            app_secret = conn.password
            token_credential = ClientSecretCredential(tenant, app_id, app_secret)
            return BlobServiceClient(account_url=conn.host, credential=token_credential, **extra)

        sas_token = extra.pop("sas_token", extra.pop("extra__wasb__sas_token", None))
        if sas_token:
            if sas_token.startswith("https"):
                return BlobServiceClient(account_url=sas_token, **extra)
            else:
                return BlobServiceClient(
                    account_url=f"https://{conn.login}.blob.core.windows.net/{sas_token}", **extra
                )

        # Fall back to old auth (password) or use managed identity if not provided.
        credential = conn.password
        if not credential:
            credential = DefaultAzureCredential()
            self.log.info("Using DefaultAzureCredential as credential")
        return BlobServiceClient(
            account_url=f"https://{conn.login}.blob.core.windows.net/",
            credential=credential,
            **extra,
        )

    def _get_blob_client(self, container_name: str, blob_name: str) -> BlobClient:
        """
        Instantiates a blob client.

        :param container_name: the name of the blob container
        :param blob_name: the name of the blob. This needs not be existing
        """
        return self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    async def check_for_blob_async(self, container_name: str, blob_name: str, **kwargs: Any) -> bool:
        """
        Check if a blob exists on Azure Blob Storage.

        :param container_name: name of the container
        :param blob_name: name of the blob
        :param kwargs: optional keyword arguments for ``BlobClient.get_blob_properties``
        """
        try:
            await self._get_blob_client(container_name, blob_name).get_blob_properties(**kwargs)
        except ResourceNotFoundError:
            return False
        return True

    def _get_container_client(self, container_name: str) -> ContainerClient:
        """
        Instantiates a container client.

        :param container_name: the name of the container
        """
        return self.blob_service_client.get_container_client(container_name)

    async def get_blobs_list_async(
        self,
        container_name: str,
        prefix: Optional[str] = None,
        include: Optional[List[str]] = None,
        delimiter: str = "/",
        **kwargs: Any,
    ) -> List[BlobProperties]:
        """
        List blobs in a given container.

        :param container_name: the name of the container
        :param prefix: filters the results to return only blobs whose names
            begin with the specified prefix.
        :param include: specifies one or more additional datasets to include in the
            response. Options include: ``snapshots``, ``metadata``, ``uncommittedblobs``,
            ``copy`, ``deleted``.
        :param delimiter: filters objects based on the delimiter (for e.g '.csv')
        """
        container = self._get_container_client(container_name)
        blob_list = []
        blobs = container.walk_blobs(name_starts_with=prefix, include=include, delimiter=delimiter, **kwargs)
        async for blob in blobs:
            blob_list.append(blob.name)
        return blob_list

    async def check_for_prefix_async(self, container_name: str, prefix: str, **kwargs: Any) -> bool:
        """
        Check if a prefix exists on Azure Blob storage.

        :param container_name: Name of the container.
        :param prefix: Prefix of the blob.
        :param kwargs: Optional keyword arguments for ``ContainerClient.walk_blobs``
        """
        blobs = await self.get_blobs_list_async(container_name=container_name, prefix=prefix, **kwargs)
        return len(blobs) > 0
