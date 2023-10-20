from __future__ import print_function
from future.utils import raise_from
import os
import sys
import datetime
import time
import requests
from lxml import etree, html, objectify
import logging
from collections import OrderedDict
import hashlib
from io import StringIO
import geopandas as gpd
import pandas as pd
import shapely.wkt as wkt
import shapely.ops as ops
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon, LineString, box
from shapely.ops import transform
import shapely
import math
import pyproj
from . import geo_shapely as geoshp
from . import geopandas_coloc
import warnings
from tqdm.auto import tqdm
import pytz
from packaging import version
import tempfile
import re
import string
import zipfile

logger = logging.getLogger("sentinelRequest")
logger.addHandler(logging.NullHandler())

if sys.gettrace():
    logger.setLevel(logging.DEBUG)
    logger.debug('logging level set to logging.DEBUG')
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)
else:
    logger.setLevel(logging.INFO)

try:
    from html2text import html2text
except:
    logger.info("html2text not found. Consider 'pip install html2text' for better error messages.")
    html2text = lambda x: x

"""# default values (user may change them)
default_user = 'guest'
default_password = 'guest'
default_cachedir = None
default_alt_path = None
default_cacherefreshrecent = datetime.timedelta(days=7)
default_timedelta_slice = datetime.timedelta(weeks=1)
default_filename = 'S1*'"""

# all wkt objects feeded to scihub will keep rounding_precision digits (1 = 0.1 )
# this will allow to not have too long requests
rounding_precision = 1

# To change
answer_fields = [u'acquisitiontype', u'beginposition', u'endposition', u'filename',
                 u'footprint', u'format', u'gmlfootprint', u'identifier',
                 u'ingestiondate', u'instrumentname', u'instrumentshortname',
                 u'lastorbitnumber', u'lastrelativeorbitnumber', u'missiondatatakeid',
                 u'orbitdirection', u'orbitnumber', u'platformidentifier',
                 u'platformname', u'polarisationmode', u'productclass', u'producttype',
                 u'relativeorbitnumber', u'sensoroperationalmode', u'size',
                 u'slicenumber', u'status', u'swathidentifier', u'url',
                 u'url_alternative', u'url_icon', u'uuid']

dateformat = "%Y-%m-%dT%H:%M:%S.%fZ"
dateformat_alt = "%Y-%m-%dT%H:%M:%S"

urlapi = 'https://apihub.copernicus.eu/apihub/search'
# urlapi = 'https://scihub.copernicus.eu/dhus/search'

# earth as multi poly
earth = GeometryCollection(list(gpd.read_file(gpd.datasets.get_path('naturalearth_lowres')).geometry)).buffer(0)

# projection used by scihub
if hasattr(pyproj, 'CRS') and version.parse(gpd.__version__) >= version.parse("0.7.0"):
    # pyproj.CRS is handled by geopandas since 0.7.0
    scihub_crs = pyproj.CRS("epsg:4326")
else:
    logger.warning("pyproj < 2.0 and geopandas < 0.7 are deprecated")
    scihub_crs = {'init': 'epsg:4326'}

# empty safe gdf
safes_empty = gpd.GeoDataFrame(columns=answer_fields, geometry='footprint', crs=scihub_crs)


class ScihubError(UserWarning):
    """class handler for ScihubError warnnings"""
    pass


# remove_dom
xslt = '''<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="xml" indent="no"/>

<xsl:template match="/|comment()|processing-instruction()">
    <xsl:copy>
      <xsl:apply-templates/>
    </xsl:copy>
</xsl:template>

<xsl:template match="*">
    <xsl:element name="{local-name()}">
      <xsl:apply-templates select="@*|node()"/>
    </xsl:element>
</xsl:template>

<xsl:template match="@*">
    <xsl:attribute name="{local-name()}">
      <xsl:value-of select="."/>
    </xsl:attribute>
</xsl:template>
</xsl:stylesheet>
'''

remove_dom = etree.XSLT(etree.fromstring(xslt))


def is_geographic(crs):
    """ return True if crs is geographic.
        once old pyproj/geopandas deprecated, this function should be replaced
        with crs.is_geographic (or pyproj.CRS(crs).is_geographic )
    """
    try:
        return pyproj.CRS(crs).is_geographic
    except:
        # old pyproj
        return pyproj.Proj(**crs).is_latlong()


def nice_string(obj):
    """try to convert obj to nice string"""
    xml = False
    string = str(obj)
    try:
        # xml ?
        try:
            # if xml string
            obj = etree.fromstring(obj)
        except:
            # assume already etree
            pass
        etree.indent(obj, space="  ")
        string = etree.tostring(obj, pretty_print=True).decode('unicode_escape')
        xml = True
    except:
        pass

    try:
        obj = obj.decode('unicode_escape')
    except:
        pass

    if not xml:
        # html string ?
        try:
            check_html = html.fromstring(obj)
            if check_html.find('.//*') is not None:
                string = html2text(obj)
        except:
            pass

    return string


"""def wget(url, outfile, progress=True, user=None, password=None, desc=''):
    # get default keywords values
    if user is None:
        user = default_user
    if password is None:
        password = default_password

    if os.path.exists(outfile):
        return 303, outfile

    response = requests.get(url, stream=True, auth=(user, password))
    if response.status_code == 202:
        # not online
        return response.status_code, None
    elif response.status_code != 200:
        logger.debug('strange status %d for %s' % (response.status_code, outfile))

    length = int(response.headers.get('content-length', 0))

    # make a tempfile
    basename = os.path.basename(outfile)
    dirname = os.path.dirname(outfile)
    prefix, suffix = os.path.splitext(basename)
    progress_bar = tqdm(total=length, unit='iB', unit_scale=True, desc=desc, leave=False, disable=not progress)
    with tempfile.NamedTemporaryFile(suffix='.tmp', prefix=prefix, dir=dirname, delete=False) as handle:
        chunk_size = 10 * 1024 ** 2
        for data in response.iter_content(chunk_size=chunk_size):
            progress_bar.update(len(data))
            handle.write(data)
        os.rename(handle.name, outfile)
    progress_bar.close()
    return response.status_code, outfile
"""

d"""ef safe_dir(filename, path='.', only_exists=False):
    """
    get dir path from safe filename.

    Parameters
    ----------
    filename: str
        SAFE filename, with no dir, and valid nomenclature
    path: str or list of str
        path template
    only_exists: bool
        if True and path doesn't exists, return None.
        if False, return last path found

    Examples
    --------
    For datarmor at ifremer, path template should be:

    '/home/datawork-cersat-public/cache/project/mpc-sentinel1/data/esa/${longmissionid}/L${LEVEL}/${BEAM}/${MISSIONID}_${BEAM}_${PRODUCT}${RESOLUTION}_${LEVEL}${CLASS}/${year}/${doy}/${SAFE}'

    For creodias, it should be:

    '/eodata/Sentinel-1/SAR/${PRODUCT}/${year}/${month}/${day}/${SAFE}'

    Returns
    -------
    str
        path from template

    """

    # this function is shared between sentinelrequest and xsar

    if 'S1' in filename:
        regex = re.compile(
            "(...)_(..)_(...)(.)_(.)(.)(..)_(........T......)_(........T......)_(......)_(......)_(....).SAFE")
        template = string.Template(
            "${MISSIONID}_${BEAM}_${PRODUCT}${RESOLUTION}_${LEVEL}${CLASS}${POL}_${STARTDATE}_${STOPDATE}_${ORBIT}_${TAKEID}_${PRODID}.SAFE")
    elif 'S2' in filename:
        # S2B_MSIL1C_20211026T094029_N0301_R036_T33SWU_20211026T115128.SAFE
        # YYYYMMDDHHMMSS: the datatake sensing start time
        # Nxxyy: the PDGS Processing Baseline number (e.g. N0204)
        # ROOO: Relative Orbit number (R001 - R143)
        # Txxxxx: Tile Number field*
        # second date if product discriminator
        regex = re.compile(
            "(...)_(MSI)(...)_(........T......)_N(....)_R(...)_T(.....)_(........T......).SAFE")
        template = string.Template(
            "${MISSIONID}_${PRODUCT}${LEVEL}_${STARTDATE}_${PROCESSINGBL}_${ORBIT}_${TIlE}_${PRODID}.SAFE")
    else:
        raise Exception('mission not handle')
    regroups = re.search(regex, filename)
    tags = {}
    for itag, tag in enumerate(re.findall(r"\$\{([\w]+)\}", template.template), start=1):
        tags[tag] = regroups.group(itag)

    startdate = datetime.datetime.strptime(
        tags["STARTDATE"], '%Y%m%dT%H%M%S').replace(tzinfo=pytz.UTC)
    tags['SAFE'] = regroups.group(0)
    tags["missionid"] = tags["MISSIONID"][1:3].lower()  # should be replaced by tags["MISSIONID"].lower()
    tags["longmissionid"] = 'sentinel-%s' % tags["MISSIONID"][1:3].lower()
    tags["year"] = startdate.strftime("%Y")
    tags["month"] = startdate.strftime("%m")
    tags["day"] = startdate.strftime("%d")
    tags["doy"] = startdate.strftime("%j")
    if isinstance(path, str):
        path = [path]
    filepath = None
    for p in path:
        # deprecation warnings (see https://github.com/oarcher/sentinelrequest/issues/4)
        if '{missionid}' in p:
            warnings.warn('{missionid} tag is deprecated. Update your path template to use {longmissionid}')
        filepath = string.Template(p).substitute(tags)
        if not filepath.endswith(filename):
            filepath = os.path.join(filepath, filename)
        if only_exists:
            if not os.path.isfile(os.path.join(filepath, 'manifest.safe')):
                filepath = None
            else:
                # a path was found. Stop iterating over path list
                break
    return filepath"""


"""def get_scihub_odata(odata_url):
    """
    get odata attribute from odata url.
    odata result are not cached, because they are volatiles (ie online status)
    """
    odata = {}
    xmlout = requests.get(odata_url, auth=(default_user, default_password))
    root = objectify.fromstring(xmlout.content)
    odata['Online'] = root.find('*/d:Online', namespaces=root.nsmap).pyval
    odata['OnDemand'] = root.find('*/d:OnDemand', namespaces=root.nsmap).pyval
    return pd.Series(odata)"""


"""def scihub_download(safe, destination='.', desc='', progress=True):
    if safe['path'] is not None and os.path.exists(os.path.join(safe['path'], 'manifest.safe')):
        # do not download existing safe
        logger.debug('no need to download %s' % safe['path'])
        safe['odata_Online'] = True
        return safe
    else:
        safe['path'] = None

    safe['odata_Online'], safe['odata_OnDemand'], safe['badzip'] = None, None, True

    parent_dir = safe_dir(safe['filename'], path=destination)
    path = os.path.join(parent_dir, safe['filename'])

    zip_dir = os.path.join(destination, 'zip')
    try:
        os.makedirs(zip_dir, exist_ok=True)
    except IOError as e:
        raise IOError("Unable to create %s : %s", (str(zip_dir), str(e)))

    status, filezip = wget(safe['url'], os.path.join(zip_dir, "%s.zip" % safe['filename']), desc=desc)
    if status == 202:
        logger.info('%s not yet online' % safe['filename'])

    if status != 200 and status != 303:
        odata = get_scihub_odata(safe['url_alternative'])
        safe['odata_Online'], safe['odata_OnDemand'] = odata['Online'], odata['OnDemand']
    safe['zipfilepath'] = filezip
    if safe['zipfilepath'] is not None:
        os.makedirs(parent_dir, exist_ok=True)
        partial_unzip = os.path.join(zip_dir, 'partial')
        os.makedirs(partial_unzip, exist_ok=True)
        try:
            with zipfile.ZipFile(safe['zipfilepath'], 'r') as zip_ref:
                for file in tqdm(iterable=zip_ref.namelist(), total=len(zip_ref.namelist()), desc='unzip',
                                 disable=not progress, leave=False):
                    zip_ref.extract(member=file, path=partial_unzip)
            safe['badzip'] = False
        except zipfile.BadZipFile:
            logger.info('remove bad zip  %s' % safe['zipfilepath'])
            os.unlink(safe['zipfilepath'])
            safe['path'] = None
        else:
            os.rename(os.path.join(partial_unzip, safe['filename']), path)
            safe['path'] = path
    else:
        logger.error('Unable to download zipfile for %s' % safe['filename'])
        safe['path'] = None
    return safe
"""

"""def download_from_df(safes, destination='.', progress=True):
    missings = safes[safes['path'].isnull()]
    logger.debug('missings:%s' % str(missings['path']))
    global_count = len(missings)
    logger.info("need to download %d/%d safes" % (global_count, len(safes)))
    download_list = []

    current = 1
    error_count = {}
    while len(missings) != 0:
        new_missings = missings
        start_len = len(download_list)
        for idx, missing in missings.iterrows():
            downloaded = scihub_download(
                missing, destination=destination,
                desc='%d/%d ' % (current, global_count), progress=progress)
            if downloaded['path']:
                current = current + 1
                download_list.append(downloaded)
                new_missings = new_missings[new_missings['filename'] != downloaded['filename']]
            else:
                if downloaded['filename'] not in error_count:
                    error_count[downloaded['filename']] = 0
                error_count[downloaded['filename']] = error_count[downloaded['filename']] + 1
                logger.warning(
                    'download failed (#%d)for %s' % (error_count[downloaded['filename']], downloaded['filename']))
                if error_count[downloaded['filename']] > 2:
                    logger.warning('to many errors on %s. Skipping' % downloaded['filename'])
                    new_missings = new_missings[new_missings['filename'] != downloaded['filename']]

        missings = new_missings
        stop_len = len(download_list)
        count = stop_len - start_len
        if count == 0 and len(missings) - len(error_count.keys()) > 0:
            logger.warning("waiting for one of %s safes to be online" %
                           len(missings))
            time.sleep(30)

    logger.info('%d safes downloaded, %d errors' % (len(download_list), len(error_count.keys())))
    safes.drop(['odata_Online', 'odata_OnDemand', 'badzip'],
               errors='ignore', inplace=True)

    return safes"""


def scihubQuery_raw(str_query, user=None, password=None, cachedir=None, cacherefreshrecent=None,
                    return_cache_status=False):
    """
    real scihub query, as done on https://scihub.copernicus.eu/dhus/#/home
    but with cache handling

    return a geodataframe with responses, or tuple (gdf,cache_status) if return_cache_status is True
    """

    # get default keywords values
    if user is None:
        user = default_user
    if password is None:
        password = default_password
    if cachedir is None:
        cachedir = default_cachedir
    if cacherefreshrecent is None:
        cacherefreshrecent = default_cacherefreshrecent

    def decode_date(strdate):
        # date format can change ..
        try:
            d = datetime.datetime.strptime(strdate, dateformat).replace(tzinfo=pytz.UTC)
        except:
            d = datetime.datetime.strptime(strdate[0:19], dateformat_alt).replace(tzinfo=pytz.UTC)
        return d

    decode_tags = {
        "int": int,
        "date": decode_date
    }

    retry_init = 3

    safes = safes_empty.copy()
    start = 0
    count = 1  # arbitrary count > start
    retry = retry_init

    cache_status = False

    if cachedir:
        os.makedirs(os.path.join(cachedir, 'xml'), exist_ok=True)

    while start < count:
        params = OrderedDict([("start", start), ("rows", 100), ("q", str_query)])
        root = None
        xml_cachefile = None
        if cachedir is not None:
            md5request = hashlib.md5(("%s" % params).encode('utf-8')).hexdigest()
            xml_cachedir = os.path.join(cachedir, 'xml', md5request[:2])
            os.makedirs(xml_cachedir, exist_ok=True)
            xml_cachefile = os.path.join(xml_cachedir, '%s.xml' % md5request[2:])

            # legacy stuff that might be removed in few months (now 202012)
            xml_cachefile_legacy = os.path.join(cachedir, "%s.xml" % md5request)
            if os.path.exists(xml_cachefile_legacy):
                logger.debug('migrating old legacy cache file')
                os.rename(xml_cachefile_legacy, xml_cachefile)

            if os.path.exists(xml_cachefile):
                logger.debug("reading from xml cachefile %s" % xml_cachefile)
                try:
                    with open(xml_cachefile, 'a'):
                        os.utime(xml_cachefile, None)
                except Exception as e:
                    logger.warning('unable to touch %s : %s' % (xml_cachefile, str(e)))

                try:
                    root = remove_dom(etree.parse(xml_cachefile))
                    int(root.find(".//totalResults").text)  # this should enought to test the xml is ok
                except Exception as e:
                    logger.warning('removing invalid xml_cachefile %s : %s' % (xml_cachefile, str(e)))
                    os.unlink(xml_cachefile)
                    root = None

        if root is not None:
            cache_status = True
        else:
            # request not cached
            try:
                xmlout = requests.get(urlapi, auth=(user, password), params=params)
            except:
                raise_from(ConnectionError("Unable to connect to %s" % urlapi), None)
            try:
                root = remove_dom(etree.fromstring(xmlout.content))
            except Exception as e:
                content = nice_string(xmlout.content)

                if 'Timeout occured while waiting response from server' in content:
                    retry -= 1
                    logger.warning('Timeout while processing request : %s' % str_query)
                    logger.warning('left retry : %s' % retry)
                    if retry == 0:
                        warnings.warn('Giving up trying to connect %s ' % urlapi, ScihubError)
                        break
                    continue

                logger.critical("Error while parsing xml answer")
                logger.critical("query was: %s" % str_query)
                logger.critical("answer is: \n {}".format(content))
                warnings.warn('Schihub query error %s ' % urlapi, ScihubError)

            if xml_cachefile is not None:
                try:
                    int(root.find(".//totalResults").text)  # this should enought to test the xml is ok
                    try:
                        with open(xml_cachefile, 'w') as f:
                            f.write(nice_string(root))
                    except Exception as e:
                        logger.warning('unable to write xml_cachefile %s : %s' % (xml_cachefile, str(e)))
                except:
                    logger.warning('not writing corrupted xml cachefile')

        # <opensearch:totalResults>442</opensearch:totalResults>\n
        try:
            count = int(root.find(".//totalResults").text)
        except:
            # there was an error in request
            logger.error('response was:\n {}'.format(nice_string(root)))
            if xml_cachefile is not None and os.path.exists(xml_cachefile):
                os.unlink(xml_cachefile)
            warnings.warn('invalid request %s ' % str_query, ScihubError)
            break

        # reset retry since last request is ok
        retry = retry_init

        # logger.debug("totalResults : %s" % root.find(".//totalResults").text )
        logger.debug("%s" % root.find(".//subtitle").text)
        # logger.debug("got %d entry starting at %d" % (len(root.findall(".//entry")),start))

        if len(root.findall(".//entry")) > 0:
            chunk_safes_df = pd.DataFrame(columns=answer_fields)
            t = time.time()
            for field in answer_fields:
                if field.startswith('url'):
                    if field == 'url':
                        elts = [d for d in root.xpath(".//entry/link") if 'rel' not in d.attrib]
                    else:
                        rel = field.split('_')[1]
                        elts = root.xpath(".//entry/link[@rel='%s']" % rel)
                    tag = 'str'
                    values = [d.attrib['href'] for d in elts]
                else:
                    elts = root.xpath(".//entry/*[@name='%s']" % field)
                    if len(elts) != 0:
                        tag = elts[0].tag  # ie str,int,date ..
                        values = [d.text for d in elts]
                    else:
                        tag = None
                        values = []
                        logger.debug("Ignoring field %s (not found)." % field)
                if len(values) >= 1:
                    chunk_safes_df[field] = values
                    if tag in decode_tags:
                        chunk_safes_df[field] = chunk_safes_df[field].apply(decode_tags[tag])
            try:
                shp_footprints = chunk_safes_df['footprint'].apply(wkt.loads)
            except:
                pass
            chunk_safes_df['footprint'] = shp_footprints
            chunk_safes = gpd.GeoDataFrame(chunk_safes_df, geometry='footprint', crs=scihub_crs)
            chunk_safes['footprint'] = chunk_safes.buffer(0)
            start += len(chunk_safes)
            logger.debug("xml parsed in %.2f secs" % (time.time() - t))

            # remove cachefile if some safes are recents
            if xml_cachefile is not None and os.path.exists(xml_cachefile):
                dateage = (datetime.datetime.utcnow().replace(tzinfo=pytz.UTC) - chunk_safes[
                    'beginposition'].max())  # used for cache age
                if dateage < cacherefreshrecent:
                    logger.debug("To recent answer. Removing cachefile %s" % xml_cachefile)
                    os.unlink(xml_cachefile)
            # sort by sensing date
            safes = pd.concat([safes, chunk_safes], ignore_index=True, sort=False)
            safes = safes.sort_values('beginposition')
            safes.reset_index(drop=True, inplace=True)
            safes = safes.set_geometry('footprint')

        # safes['footprint'] = gpd.GeoSeries(safes['footprint'])
        safes.crs = scihub_crs

    if return_cache_status:
        return safes, cache_status
    else:
        return safes


def _colocalize(safes, gdf, crs=scihub_crs, coloc=[geopandas_coloc.colocalize_loop], progress=False):
    """colocalize safes and gdf
    if crs is default and 'geometry_east' and 'geometry_west' exists in gdf,
    they will be used instead of .geometry (scihub mode)

    if crs is not default the crs will be used on .geometry for the coloc.

    the returned safes will be returned in scihub crs (ie 4326 : not the user specified)
    """

    # initialise an empty index for both gdf
    idx_safes = safes.index.delete(slice(None))
    idx_gdf = gdf.index.delete(slice(None))
    logger.info('========= safes : %s', safes)
    if len(safes) == 0:
        # set same index as gdf, even if empty, to not throw an error on possible merge later
        safes.index = idx_gdf
        return safes
    gdf = gdf.copy()
    gdf['geometry'] = gdf.geometry
    gdf['startdate'] = gdf['beginposition']
    gdf['stopdate'] = gdf['endposition']
    safes['startdate'] = safes['beginposition']
    safes['stopdate'] = safes['endposition']

    safes_coloc = safes.iloc[0:0, :].copy()
    safes_coloc.crs = scihub_crs
    scihub_mode = False

    safes_crs = safes.copy()

    geometry_list = ['geometry']
    if is_geographic(crs) and 'geometry_east' in gdf and 'geometry_west' in gdf:
        # never reached. replaced with 'scihub_geometry_east_list'
        raise DeprecationWarning('This should be deprecated')
        scihub_mode = True
        # remove unused geometry
        old_geometry = gdf.geometry.name
        gdf.set_geometry('geometry_east', inplace=True)
        gdf.drop(labels=[old_geometry], inplace=True, axis=1)
        geometry_list = ['geometry_east', 'geometry_west']
    elif not is_geographic(crs):
        gdf.set_geometry('geometry', inplace=True)
        gdf.to_crs(crs, inplace=True)
        safes_crs.to_crs(crs, inplace=True)
        safes_coloc.to_crs(crs, inplace=True)

    for geometry in geometry_list:
        t = time.time()
        idx_safes_cur, idx_gdf_cur = coloc[0](safes_crs, gdf.set_geometry(geometry), progress=progress)
        logger.debug('sub coloc %s done in %ds' % (coloc[0].__name__, time.time() - t))
        idx_safes = idx_safes.append(idx_safes_cur)
        idx_gdf = idx_gdf.append(idx_gdf_cur)
        for imethod in range(1, len(coloc)):
            # check with other coloc method
            t = time.time()
            idx_safes_cur_check, idx_gdf_cur_check = coloc[imethod](safes_crs, gdf.set_geometry(geometry),
                                                                    progress=progress)
            logger.debug('sub coloc %s done in %.1fs' % (coloc[imethod].__name__, time.time() - t))
            if not (idx_gdf_cur_check.sort_values().equals(
                idx_gdf_cur.sort_values()) and idx_safes_cur_check.sort_values().equals(
                idx_safes_cur.sort_values())):
                raise RuntimeError('difference between colocation method')

    safes_coloc = safes.loc[idx_safes]
    safes_coloc.index = idx_gdf

    return safes_coloc.drop(['startdate', 'stopdate'], axis=1).to_crs(crs=scihub_crs)


def remove_duplicates(safes_ori, keep_list=[]):
    """
    Remove duplicate safe (ie same footprint with same date, but different prodid)
    """
    safes = safes_ori.copy()
    if not safes.empty:
        # remove duplicate safes

        # add a temporary col with filename radic
        safes['__filename_radic'] = [f[0:62] for f in safes['filename']]

        uniques_radic = safes['__filename_radic'].unique()

        for filename_radic in uniques_radic:
            sames_safes = safes[safes['__filename_radic'] == filename_radic]
            if len(sames_safes['filename'].unique()) > 1:
                logger.debug("prodid count > 1: %s" % ([s for s in sames_safes['filename'].unique()]))
                force_keep = list(set(sames_safes['filename']).intersection(keep_list))
                to_keep = sames_safes[
                    'ingestiondate'].max()  # warning : may induce late reprocessing (ODL link) . min() is safer, but not the best quality

                if force_keep:
                    _to_keep = sames_safes[sames_safes['filename'] == force_keep[0]]['ingestiondate'].iloc[0]
                    if _to_keep != to_keep:
                        logger.warning('remove_duplicate : force keep safe %s' % force_keep[0])
                        to_keep = _to_keep
                logger.debug("only keep : %s " % set([f for f in safes[safes['ingestiondate'] == to_keep]['filename']]))
                safes = safes[(safes['ingestiondate'] == to_keep) | (safes['__filename_radic'] != filename_radic)]

        safes.drop('__filename_radic', axis=1, inplace=True)
    return safes


def get_datatakes(safes, datatake=0, user=None, password=None, cachedir=None, cacherefreshrecent=None):
    # get default keywords values
    if user is None:
        user = default_user
    if password is None:
        password = default_password
    if cachedir is None:
        cachedir = default_cachedir
    if cacherefreshrecent is None:
        cacherefreshrecent = default_cacherefreshrecent

    safes['datatake_index'] = 0
    for safe in list(safes['filename']):
        safe_index = safes[safes['filename'] == safe].index[0]
        takeid = safe.split('_')[-2]
        safe_rad = "_".join(safe.split('_')[0:4])
        safes_datatake = scihubQuery_raw('filename:%s_*_*_*_%s_*' % (safe_rad, takeid), user=user, password=password,
                                         cachedir=cachedir, cacherefreshrecent=cacherefreshrecent)
        # FIXME duplicate are removed, even if duplicate=True
        safes_datatake = remove_duplicates(safes_datatake, keep_list=[safe])

        try:
            ifather = safes_datatake[safes_datatake['filename'] == safe].index[0]
        except:
            logger.warn('Father safe was not the most recent one (scihub bug ?)')

        # ifather=safes_datatake.index.get_loc(father) # convert index to iloc

        safes_datatake['datatake_index'] = safes_datatake.index - ifather

        # get adjacent safes
        safes_datatake = safes_datatake[abs(safes_datatake['datatake_index']) <= datatake]

        # set same index as father safe
        safes_datatake.set_index(pd.Index([safe_index] * len(safes_datatake)), inplace=True)

        # remove datatake allready in safes (ie father and allready colocated )
        for safe_datatake in safes_datatake['filename']:
            if (safes['filename'] == safe_datatake).any():
                # FIXME take the lowest abs(datatake_index)
                safes_datatake = safes_datatake[safes_datatake['filename'] != safe_datatake]

        safes = safes.append(safes_datatake, sort=False)
    return safes


def normalize_gdf(gdf, startdate=None, stopdate=None, date=None, dtime=None, timedelta_slice=None, progress=False):
    """ return a normalized gdf list
    start/stop date name will be 'beginposition' and 'endposition'
    """
    t = time.time()
    if timedelta_slice is None:
        timedelta_slice = default_timedelta_slice
    if gdf is not None:
        if not gdf.index.is_unique:
            raise IndexError("Index must be unique. Duplicate founds : %s" % list(
                gdf.index[gdf.index.duplicated(keep=False)].unique()))
        if len(gdf) == 0:
            return []
        norm_gdf = gdf.copy()
    else:
        norm_gdf = gpd.GeoDataFrame({
            'beginposition': startdate,
            'endposition': stopdate,
            'geometry': Polygon()
        }, geometry='geometry', index=[0], crs=scihub_crs)
        # no slicing
        timedelta_slice = None

    # convert naives dates to utc
    for date_col in norm_gdf.select_dtypes(include=['datetime64']).columns:
        try:
            norm_gdf[date_col] = norm_gdf[date_col].dt.tz_localize('UTC')
            logger.warning("Assuming UTC date on col %s" % date_col)
        except TypeError:
            # already localized
            pass

    # check valid input geometry
    if not all(norm_gdf.is_valid):
        raise ValueError("Invalid geometries found. Check them with gdf.is_valid")

    norm_gdf['wrap_dlon'] = False

    crs_ori = norm_gdf.crs
    if crs_ori is None:
        logger.warning('no crs provided. assuming lon/lat with greenwich/antimeridian handling')
        norm_gdf['wrap_dlon'] = norm_gdf.geometry.apply(lambda s: not hasattr(s, '__iter__'))
        norm_gdf.geometry = norm_gdf.geometry.apply(geoshp.smallest_dlon)
        norm_gdf.crs = scihub_crs

    # scihub requests are enlarged/simplified
    if is_geographic(norm_gdf.crs):
        buff = 2
        simp = 1.9
    else:
        # assume meters
        buff = 200 * 1000
        simp = 190 * 1000

    with warnings.catch_warnings():
        # disable geographic warning
        warnings.simplefilter("ignore")
        norm_gdf['scihub_geometry'] = norm_gdf.geometry.buffer(buff).simplify(simp)
    if crs_ori is None:
        # re apply smallest dlon if needed
        norm_gdf['scihub_geometry'] = norm_gdf.set_geometry('scihub_geometry').apply(
            lambda row: geoshp.smallest_dlon(row['scihub_geometry']) if row['wrap_dlon'] else GeometryCollection(
                [row['scihub_geometry']]),
            axis=1)

    if not is_geographic(norm_gdf.crs):
        # convert scihub geometry to lon/lat (original geometry untouched !)
        norm_gdf_ori = norm_gdf.copy()
        crs_ori = norm_gdf.crs
        norm_gdf['scihub_geometry'] = norm_gdf.set_geometry('scihub_geometry').geometry.apply(
            lambda s: geoshp.split_shape_crs(s, crs=norm_gdf.crs))
        norm_gdf['scihub_geometry'] = norm_gdf.set_geometry('scihub_geometry').geometry.to_crs(scihub_crs)
        # norm_gdf['scihub_geometry'] =

        # check valid output geometry
        if not all(norm_gdf.set_geometry('scihub_geometry').geometry.is_valid):
            raise NotImplementedError("Internal error converting crs %s to %s" % (norm_gdf.crs, scihub_crs))
            # an output geometry is invalid if it include 4326 singularity (ie pole)
            all_count = len(norm_gdf)
            valid = norm_gdf.is_valid

            # split into geometry collection that doesn't include singularity
            corrected = norm_gdf_ori[~valid].geometry.apply(lambda s: geoshp.split_shape_crs(s, crs=norm_gdf_ori.crs))

            norm_gdf.loc[~valid, norm_gdf.geometry.name] = corrected.to_crs(scihub_crs)
            if not all(norm_gdf.is_valid):
                raise ValueError("unable to convert to crs %s" % scihub_crs)

            logging.error("Converted %s/%s problematic projection %s -> %s geometries " % (
                len(corrected), all_count, crs_ori, scihub_crs))

        # encapsulate geometry in collection to presereve large dlon
        norm_gdf['scihub_geometry'] = norm_gdf.set_geometry('scihub_geometry').geometry.apply(
            lambda s: GeometryCollection([s]))

    # else:
    #    norm_gdf.geometry = norm_gdf.geometry.apply(smallest_dlon)
    east, west = zip(*norm_gdf.set_geometry('scihub_geometry').geometry.apply(geoshp.split_east_west))
    norm_gdf['scihub_geometry_east_list'] = list(east)
    norm_gdf['scihub_geometry_west_list'] = list(west)

    if date in norm_gdf:
        if (startdate not in norm_gdf) and (stopdate not in norm_gdf):
            norm_gdf['beginposition'] = norm_gdf[date] - dtime
            norm_gdf['endposition'] = norm_gdf[date] + dtime
        else:
            raise ValueError('date keyword conflict with startdate/stopdate')

    if (startdate in norm_gdf) and (startdate != 'beginposition'):
        norm_gdf['beginposition'] = norm_gdf[startdate]

    if (stopdate in norm_gdf) and (stopdate != 'endposition'):
        norm_gdf['endposition'] = norm_gdf[stopdate]

    gdf_slices = norm_gdf
    # slice
    if timedelta_slice is not None:
        mindate = norm_gdf['beginposition'].min()
        maxdate = norm_gdf['endposition'].max()
        # those index will need to be time expanded
        idx_to_expand = norm_gdf.index[(norm_gdf['endposition'] - norm_gdf['beginposition']) > timedelta_slice]
        if maxdate > datetime.datetime.utcnow().replace(tzinfo=pytz.UTC):
            logger.info("%s is future. Truncating." % maxdate)
            maxdate = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC) + datetime.timedelta(days=1)
        if (mindate == mindate) and (maxdate == maxdate):  # non nan
            gdf_slices = []
            slice_begin = mindate
            slice_end = slice_begin
            islice = 0
            nslices = math.ceil((maxdate - mindate) / timedelta_slice)
            if nslices > 1:
                logger.info("Slicing into %d chunks of %s ..." % (nslices, timedelta_slice))
            while slice_end < maxdate:
                islice += 1
                slice_end = slice_begin + timedelta_slice
                # this is time grouping
                gdf_slice = norm_gdf[
                    (norm_gdf['beginposition'] >= slice_begin) & (norm_gdf['endposition'] <= slice_end)]
                # check if some slices needs to be expanded
                # index of gdf_slice that where not grouped
                not_grouped_index = pd.Index(set(idx_to_expand) - set(gdf_slice.index))
                for to_expand in not_grouped_index:
                    # missings index in gdf_slice.
                    # check if there is time overlap.
                    latest_start = max(norm_gdf.loc[to_expand].beginposition, slice_begin)
                    earliest_end = min(norm_gdf.loc[to_expand].endposition, slice_end)
                    overlap = (earliest_end - latest_start)
                    if overlap >= datetime.timedelta(0):
                        # logger.debug("Slicing time for %s : %s to %s" % (to_expand,latest_start,earliest_end))
                        gdf_slice = pd.concat([gdf_slice, gpd.GeoDataFrame(norm_gdf.loc[to_expand]).T])
                        gdf_slice.loc[to_expand, 'beginposition'] = latest_start
                        gdf_slice.loc[to_expand, 'endposition'] = earliest_end
                    # else:
                    # logger.debug("no time slice for %s in range %s to %s" % (to_expand,slice_begin,slice_end))
                logger.debug("Slice {islice:3d} : {ngeoms:3d} geometries".format(islice=islice, ngeoms=len(gdf_slice)))
                if not gdf_slice.empty:
                    gdf_slices.append(gdf_slice)
                slice_begin = slice_end

            if nslices > 1:
                logger.info(
                    'Slicing done in %.1fs . %d/%d non empty slices.' % (time.time() - t, len(gdf_slices), nslices))
    return gdf_slices


def scihubQuery(gdf=None, startdate=None, stopdate=None, date=None, dtime=None, timedelta_slice=None, filename=None,
                datatake=0, duplicate=False, query=None, user=None, password=None, min_sea_percent=None, fig=None,
                cachedir=None, cacherefreshrecent=None, progress=True, verbose=False, full_fig=False, alt_path=None,
                download=False):
    """

    input:
        gdf :
            None or geodataframe with geometry and date. gdf usually contain almost these cols:
            index         : an index for the row (for ex area name, buoy id, etc ...)
            beginposition : datetime object (startdate)
            endposition   : datetime object (stopdate)
            geometry      : shapely object (this one is optional for whole earth)
        date:
            column name if gdf, or datetime object
        dtime :
            if date is not None, dtime as timedelta object will be used to compute startdate and stopdate
        startdate :
            None or column  name in gdf , or datetime object . not used if date and dtime are defined.
            Default to 'beginposition'
        stopdate :
            None or column  name in gdf , or datetime object . not used if date and dtime are defined.
            Default to 'endposition'
        timedelta_slice:
            Max time slicing : Scihub request will be grouped or sliced to this.
            Default to datetime.timedelta(weeks=1).
            If None, no slicing is done.
        duplicate :
            if True, will return duplicates safes (ie same safe with different prodid). Default to False
        datatake :
            number of adjacent safes to return (ie 0 will return 1 safe, 1 return 3, 2 return 5, etc )
        query :
            aditionnal query string, for ex '(platformname:Sentinel-1 AND sensoroperationalmode:WV)'
        cachedir :
            cache requests for speed up.
        cacherefreshrecent :
            timedelta from now. if requested stopdate is recent, will refresh the cache to let scihub ingest new data.
            Default to datetime.timedelta(days=7).
        fig :
            matplotlib fig handle ( default to None : no plot)
        progress : True show progressbar
        verbose  : False to silent messages
        alt_path : None, str or list of str
            search path in str or list of str to get safe path (columns 'path')
            str is a path string, with optionnal wilcards like `/home/datawork-cersat-public/cache/project/mpc-sentinel1/data/esa/sentinel-${missionid}/L${LEVEL}/${BEAM}/${MISSIONID}_${BEAM}_${PRODUCT}${RESOLUTION}_${LEVEL}${CLASS}/${year}/${doy}/${SAFE}`,
            or a simple path like '.' or '/tmp/scihub_download'
            if list, search in the list until a path is found.
        download : bool
            imply get_path. default to False. If True, download safes to `alt_path` (if alt_path is a list, the first index is used)
        download_wait : bool
            if `download`, will wait for non online safe to be ready. default to False.
    return :
        a geodataframe with safes from scihub, colocated with input gdf (ie same index)
    """
    global default_user
    global default_password

    if sys.gettrace():
        logger.setLevel(logging.DEBUG)
        progress = False
        full_fig = True
    if gdf is not None and len(gdf) == 0:
        logger.warning("No coloc with an empty gdf")
        return safes_empty
    if not sys.stderr.isatty() and "tqdm.std" in str(tqdm):
        progress = False

    # get default keywords values
    if user is None:
        user = default_user
    if password is None:
        password = default_password
    # set default user/password
    default_user = user
    default_password = password

    if cachedir is None:
        cachedir = default_cachedir
    if alt_path is None:
        alt_path = default_alt_path
    if cacherefreshrecent is None:
        cacherefreshrecent = default_cacherefreshrecent
    if timedelta_slice is None:
        timedelta_slice = default_timedelta_slice
    if filename is None:
        filename = default_filename

    gdflist = normalize_gdf(gdf, startdate=startdate, stopdate=stopdate, date=date, dtime=dtime,
                            timedelta_slice=timedelta_slice)
    safes_list = []  # final request
    safes_not_colocalized_list = []  # raw request
    safes_sea_ok_list = []
    safes_sea_nok_list = []
    scihub_shapes_chunk = []
    user_shapes = []

    # user crs will be used for coloc
    if gdf is None or gdf.crs is None:
        crs = scihub_crs
    else:
        crs = gdf.crs

    # decide if loop is over dataframe or over rows
    if isinstance(gdflist, list):
        iter_gdf = gdflist
    else:
        iter_gdf = gdflist.itertuples()

    idx = 0
    pbar = tqdm(iter_gdf, total=len(gdflist), disable=not progress, leave=False)
    ncolocs = 0  # coloc count, for tqdm
    for gdf_slice in pbar:
        idx += 1
        if isinstance(gdf_slice, tuple):
            gdf_slice = gpd.GeoDataFrame([gdf_slice], index=[gdf_slice.Index])  # .reindex_like(gdf) # only one row

        if gdf_slice.empty:
            continue

        q = []
        footprint = ""
        datePosition = ""

        # get min/max date
        mindate = gdf_slice['beginposition'].min()
        maxdate = gdf_slice['endposition'].max()
        if (mindate == mindate) and (maxdate == maxdate):  # non nan
            datePosition = "beginPosition:[%s TO %s]" % (mindate.strftime(dateformat), maxdate.strftime(
                dateformat))  # shorter request . endPosition is just few seconds in future
            q.append(datePosition)

        q.append("filename:%s" % filename)

        if query:
            q.append("(%s)" % query)

        shape_east_list = list(filter(bool, gdf_slice['scihub_geometry_east_list']))
        shape_west_list = list(filter(bool, gdf_slice['scihub_geometry_west_list']))

        shape_east = Polygon()
        shape_west = Polygon()

        if shape_east_list:
            shape_east = ops.unary_union(gdf_slice['scihub_geometry_east_list']).buffer(2).simplify(1.9)
        if shape_west_list:
            shape_west = ops.unary_union(gdf_slice['scihub_geometry_west_list']).buffer(2).simplify(1.9)

        wkt_shapes = []

        for shape, plan in zip([shape_east, shape_west], [geoshp.plan_east, geoshp.plan_west]):
            if not shape.is_empty:

                # round the shape
                scihub_shape_round = wkt.loads(wkt.dumps(shape, rounding_precision=rounding_precision))

                # ensure valid coords after rounding
                try:
                    scihub_shape = scihub_shape_round.intersection(plan)
                    wkt_shapes.append(wkt.dumps(scihub_shape, rounding_precision=rounding_precision))
                except:
                    # no rounding
                    wkt_shapes.append(wkt.dumps(scihub_shape))

                scihub_shapes_chunk.append(scihub_shape)

        footprints = ['footprint:\"Intersects(%s)\" ' % wkt_shape for wkt_shape in wkt_shapes]

        if footprints:
            q.append('(%s)' % ' OR '.join(footprints))

        str_query = ' AND '.join(q)
        logger.debug("query: %s" % str_query)

        if len(
            str_query) > 8000:  # (https://scihub.copernicus.eu/twiki/do/view/SciHubUserGuide/OpenSearchAPI#Discover_the_products_over_a_pre)
            logger.error("query: %s" % str_query)
            logger.error('skipping to long query (%s > 8000)' % len(str_query))
            continue

        t = time.time()
        safes_unfiltered, cache_status = scihubQuery_raw(str_query, user=user, password=password, cachedir=cachedir,
                                                         cacherefreshrecent=cacherefreshrecent,
                                                         return_cache_status=True)
        elapsed_request = time.time() - t
        safes_unfiltered_count = len(safes_unfiltered)
        logger.debug("requested safes from scihub : %s (%.2f secs)" % (safes_unfiltered_count, elapsed_request))

        elapsed_coloc = 0
        if gdf is not None:
            t = time.time()
            if 'filename:S1' in str_query:
                # some buggy safes on scihub have stopdate < startdate : remove them
                safes_unfiltered = safes_unfiltered[
                    safes_unfiltered['endposition'] - safes_unfiltered['beginposition'] > datetime.timedelta(0)]
            logger.info('safes_unfiltered : %s', safes_unfiltered)
            safes = _colocalize(safes_unfiltered, gdf_slice, crs=crs, progress=False)
            elapsed_coloc = time.time() - t
            logger.debug("colocated with user query : %s SAFES in %.1f secs" % (len(safes), elapsed_coloc))
        else:
            # no geometry, so whole earth, and no index from gdf
            safes = safes_unfiltered.copy()
        safes_not_colocalized = safes_unfiltered[~safes_unfiltered['filename'].isin(safes['filename'])]
        del safes_unfiltered

        if not duplicate:
            nsafes = len(safes)
            safes = remove_duplicates(safes)
            logger.debug("removed %s duplicates" % (nsafes - len(safes)))

        # datatake collection to be done after colocalisation
        if datatake != 0:
            logger.debug("Asking for same datatakes")
            nsafes = len(safes)
            safes = get_datatakes(safes, datatake=datatake, user=user, password=password, cachedir=cachedir,
                                  cacherefreshrecent=cacherefreshrecent)
            logger.debug("added %s datatakes" % (len(safes) - nsafes))

            if not duplicate:
                nsafes = len(safes)
                safes = remove_duplicates(safes)
                logger.debug("removed %s duplicates" % (nsafes - len(safes)))

        if min_sea_percent is not None:
            with warnings.catch_warnings():
                # disable geographic warning
                warnings.simplefilter("ignore")
                safes_sea_percent = (safes.area - safes.intersection(earth).area) / safes.area * 100
            safes_sea_ok = safes[safes_sea_percent >= min_sea_percent]
            safes_sea_nok = safes[safes_sea_percent < min_sea_percent]
            safes = safes_sea_ok

        # sort by sensing date
        safes = safes.sort_values('beginposition')
        if gdf is not None:
            if cache_status:
                cache_str = "cache"
            else:
                cache_str = "http "
            time_str = ""
            if elapsed_request > 2 or elapsed_coloc > 2:
                time_str = " Times : req {treq:2.1f}s".format(treq=elapsed_request)
                if elapsed_coloc > 2:
                    time_str += ", coloc {tcoloc:2.1f}s".format(tcoloc=elapsed_coloc)

            status_msg = "Req {ireq:3d}/{nreq:3d} ( {chunk_size:3d} items ) : {nsafes_ok:3d}/{nsafes:3d} SAFES ({cache_status}) -> {ncoloc:4d} colocs. {time_str}".format(
                chunk_size=len(gdf_slice),
                ireq=idx, nreq=len(gdflist), nsafes_ok=len(safes['filename'].unique()),
                cache_status=cache_str,
                nsafes=safes_unfiltered_count, ncoloc=len(safes['filename']),
                time_str=time_str)
            ncolocs += len(safes)
            pbar.set_description("coloc : %04d" % ncolocs)
            if verbose:
                tqdm.write(status_msg, file=sys.stderr)
            else:
                logger.debug(status_msg)

        safes_list.append(safes)
        if full_fig:
            safes_not_colocalized_list.append(safes_not_colocalized)
            if min_sea_percent is not None:
                safes_sea_ok_list.append(safes_sea_ok)
                safes_sea_nok_list.append(safes_sea_nok)
    pbar.close()
    safes = pd.concat(safes_list, sort=False)
    safes = safes.sort_values('beginposition')
    if full_fig:
        safes_not_colocalized = pd.concat(safes_not_colocalized_list, sort=False)
        if min_sea_percent is not None:
            safes_sea_ok = pd.concat(safes_sea_ok_list, sort=False)
            safes_sea_nok = pd.concat(safes_sea_nok_list, sort=False)

    try:
        srs = crs.srs
    except:
        srs = crs['init']  # should be deprecated
    logger.info("Total : %s SAFES colocated with %s (%s uniques)." % (len(safes), srs, len(safes['filename'].unique())))

    if fig is not None:
        uniques_safes = safes.drop_duplicates('filename')
        import matplotlib.pyplot as plt
        import matplotlib as mpl
        ax = fig.add_subplot(111)
        handles = []
        if gdf is not None:
            # gdf_sel=gpd.GeoDataFrame({'geometry':user_shapes},crs=scihub_crs)
            # gdf_sel.to_crs(crs=crs,inplace=True)
            # gdf_sel.geometry.plot(ax=ax, color='none' , edgecolor='green',zorder=3)
            # original user request # TODO other color for invalid ones ?
            # decide if loop is over dataframe or over rows
            if isinstance(gdflist, list):
                all_user_geom = pd.concat(gdflist)
            else:
                all_user_geom = gdflist
            all_user_geom_shp = all_user_geom.geometry
            if not all(all_user_geom_shp.is_empty):
                all_user_geom_shp.reset_index(drop=True).explode().plot(ax=ax, color='none', edgecolor='green',
                                                                        zorder=3)
                handles.append(mpl.lines.Line2D([], [], color='green', label='user request'))

        if full_fig:
            if scihub_shapes_chunk:
                # logger.info("scihub_shapes_chunk : %s" % str(scihub_shapes_chunk))
                gdf_sel = gpd.GeoDataFrame({'geometry': scihub_shapes_chunk}, crs=scihub_crs)
                gdf_sel.to_crs(crs=crs, inplace=True)  # todo : check valid
                gdf_sel.geometry.buffer(0).plot(ax=ax, color='none', edgecolor='red', zorder=4)
                handles.append(mpl.lines.Line2D([], [], color='red', label='scihub request'))

            if len(safes_not_colocalized) > 0:
                safes_not_colocalized.geometry.apply(geoshp.smallest_dlon).to_crs(crs=crs).buffer(0).plot(ax=ax,
                                                                                                          color='none',
                                                                                                          edgecolor='orange',
                                                                                                          zorder=1,
                                                                                                          alpha=0.2)
                handles.append(mpl.lines.Line2D([], [], color='orange', label='not colocated'))
            if min_sea_percent is not None and len(safes_sea_nok) > 0:
                safes_sea_nok.geometry.apply(geoshp.smallest_dlon).to_crs(crs=crs).buffer(0).plot(ax=ax, color='none',
                                                                                                  edgecolor='olive',
                                                                                                  zorder=1, alpha=0.2)
                handles.append(mpl.lines.Line2D([], [], color='olive', label='sea area > %s %%' % min_sea_percent))

        if len(uniques_safes) > 0:
            if 'datatake_index' in uniques_safes:
                uniques_safes[uniques_safes['datatake_index'] == 0].geometry.apply(geoshp.smallest_dlon).to_crs(
                    crs=crs).buffer(0).plot(ax=ax, color='none', edgecolor='blue', zorder=2, alpha=0.7)
                uniques_safes[uniques_safes['datatake_index'] != 0].geometry.apply(geoshp.smallest_dlon).to_crs(
                    crs=crs).buffer(0).plot(ax=ax, color='none', edgecolor='cyan', zorder=2, alpha=0.2)
                handles.append(mpl.lines.Line2D([], [], color='cyan', label='datatake'))
            else:
                uniques_safes.geometry.apply(geoshp.smallest_dlon).to_crs(crs=crs).buffer(0).plot(ax=ax, color='none',
                                                                                                  edgecolor='blue',
                                                                                                  zorder=2, alpha=0.7)

            handles.append(mpl.lines.Line2D([], [], color='blue', label='colocated'))

        continents = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
        continents.to_crs(crs=crs).plot(ax=ax, zorder=0, color='gray', alpha=0.2)

        bounds = None
        try:
            # disable shapely errors, they are catched
            old_log = shapely.geos.LOG.getEffectiveLevel()
            shapely.geos.LOG.setLevel(logging.CRITICAL)
            bounds = gpd.GeoDataFrame({'geometry':
                                           [box(*uniques_safes.total_bounds),
                                            box(*safes_not_colocalized.total_bounds)] + scihub_shapes_chunk
                                       }, crs=scihub_crs).to_crs(crs=crs).buffer(0).total_bounds
            shapely.geos.LOG.setLevel(old_log)
        except Exception as e:
            logger.debug("bounds fallback : %s" % str(e))
            try:
                bounds = gpd.GeoDataFrame({'geometry': scihub_shapes_chunk
                                           }, crs=scihub_crs).to_crs(crs=crs).buffer(0).total_bounds
            except Exception as err:
                logger.debug("bounds last fallback failed: %s" % str(e))
                bounds = None

        if bounds is not None:
            xmin, xmax = ax.get_xlim()
            ymin, ymax = ax.get_ylim()
            ax.set_ylim([max(ymin, bounds[1]), min(ymax, bounds[3])])
            ax.set_xlim([max(xmin, bounds[0]), min(xmax, bounds[2])])
        fig.tight_layout()
        bbox = ax.get_position()
        ax.set_position([bbox.x0, bbox.y0, bbox.width, bbox.height * 0.8])

        ax.legend(handles=handles, loc='lower center', bbox_to_anchor=(0.5, 1.05), ncol=5)

    if cachedir is not None:
        search_paths = [cachedir]
        if alt_path is not None:
            search_paths.append(alt_path)
        # try to find already existing path
        logger.debug('search paths: %s' % str(search_paths))
        safes['path'] = safes['filename'].apply(lambda safe: safe_dir(safe, path=search_paths, only_exists=True))

    if download:
        safes = download_from_df(safes, destination=cachedir, progress=progress)

    return safes


scihubQuery_new = scihubQuery
