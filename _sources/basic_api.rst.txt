.. _basic_api:

####################
methods descriptions
####################

..
    to document functions, add them to __all__ in ../cdsodatacli/__init__.py

.. automodule:: cdsodatacli
    :members:


.. automodule:: cdsodatacli.query
    :members: fetch_data

.. automodule:: cdsodatacli.download
    :members: download_list_product_sequential, download_list_product, download_list_product_multithread_v4

.. automodule:: cdsodatacli.utils
    :members: get_conf, check_safe_in_outputdir, check_safe_in_spool, WhichArchiveDir, check_safe_in_archive, convert_json_opensearch_query_to_listing_safe_4_dowload, convert_json_odata_query_to_listing_safe_4_download

.. automodule:: cdsodatacli.sessions
    :members: get_list_active_session, get_a_free_account, write_active_session_semphore_file, remove_semaphore_session_file, get_sessions_download_available

.. automodule:: cdsodatacli.s3_temporary_access_token
    :members: _get_fresh_s3_client
