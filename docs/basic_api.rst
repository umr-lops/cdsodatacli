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
    :members: download_list_product_multithread_v4, cds_s3_download_one_product, filter_product_already_present, add_missing_cdse_hash_ids_in_listing

.. automodule:: cdsodatacli.utils
    :members: get_conf, check_safe_in_outputdir, check_safe_in_spool, WhichArchiveDir, check_safe_in_archive, convert_json_opensearch_query_to_listing_safe_4_dowload, convert_json_odata_query_to_listing_safe_4_download

.. automodule:: cdsodatacli.session
    :members: get_sessions_download_available_s3

.. automodule:: cdsodatacli.s3_temporary_access_token
    :members: _get_fresh_s3_client
