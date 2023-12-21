import numpy as np
import shapely.wkt as wkt
import shapely.ops as ops
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon, LineString, Point, MultiPoint, box
from shapely.ops import transform
from functools import partial
import sys
import logging
import pyproj

logging.basicConfig()
logger = logging.getLogger('geo_shapely')
logger.setLevel(logging.ERROR)

if sys.gettrace():
    logger.setLevel(logging.DEBUG)

if hasattr(pyproj, 'CRS'):
    crs_geographic = pyproj.CRS("epsg:4326")
else:
    crs_geographic = {'init': 'epsg:4326'}

plan_map = box(-180, -90, 180, 90)
plan_east = box(-180, -90, 0, 90)
plan_west = box(0, -90, 180, 90)
plan_east1 = box(180, -90, 360, 90)
plan_west1 = box(-180, -90, -360, 90)


def is_geographic(crs):
    """ return True if crs is geographic.
        for old pyproj/geopandas, only check 'init'
        once old pyproj/geopandas deprecated, this function should be replaced
        with crs.is_geographic (or pyproj.CRS(crs).is_geographic )
    """
    try:
        return pyproj.CRS(crs).is_geographic
    except:
        # old pyproj
        return pyproj.Proj(**crs).is_latlong()


def shape360(lon, lat):
    """shapely shape to 0 360 (for shapely.ops.transform)"""
    orig_type = type(lon)
    lon = np.array(lon) % 360
    return tuple([(orig_type)(lon), lat])


def shape180(lon, lat):
    """shapely shape to -180 180 (for shapely.ops.transform)"""
    orig_type = type(lon)

    lon = np.array(lon) % 360
    change = lon > 180

    lon[change] = lon[change] - 360

    # check if 180 should be changed to -180
    if np.sum(lon[lon != 180.0]) < 0:
        lon[lon == 180.0] = -180.0

    return tuple([(orig_type)(lon), lat])


def split_east_west(shape):
    """
        given a shape in range -360 360, return a tuple (shape_east,shape_west)
    """

    area = shape.area

    if not shape.is_valid:
        logger.warning('invalid geometry corrected by buffer(0)')
        shape = shape.buffer(0)

    shapes_east_list = []
    shapes_west_list = []

    if not hasattr(shape, '__iter__'):
        shape = [shape]

    for s in shape:
        shapes_east = []
        shapes_west = []
        shapes_east = [transform(shape180, s.intersection(east)) for east in [plan_east, plan_east1]]
        shapes_west = [transform(shape180, s.intersection(west)) for west in [plan_west, plan_west1]]
        shapes_east_list.extend(shapes_east)
        shapes_west_list.extend(shapes_west)

    shapes_east_list = list(filter(lambda s: not s.is_empty and (s.area > 0 or area == 0), shapes_east_list))
    shapes_west_list = list(filter(lambda s: not s.is_empty and (s.area > 0 or area == 0), shapes_west_list))

    shape_east = ops.unary_union(shapes_east_list)
    shape_west = ops.unary_union(shapes_west_list)

    return shape_east, shape_west


def split_shape_crs(shape, crs=None):
    # we will assume that 0,0 is singularity (ie 0,0 in crs is pole :
    # if this assertion is not valid, the split is not needed, but it will works if splitted)
    if shape.is_empty:
        return shape
    is_latlong = False
    if crs is not None:
        is_latlong = is_geographic(crs)
    if is_latlong:
        r = 180
        c = 0.1
    else:
        r = 40000 * 1000
        c = 0.1  # not 0
    ur = shape.intersection(box(c, c, r, r))
    ul = shape.intersection(box(c, -c, r, -r))
    ll = shape.intersection(box(-c, -c, -r, -r))
    lr = shape.intersection(box(-c, c, -r, r))

    res = GeometryCollection([ur, ul, ll, lr])
    if res.is_empty or not res.is_valid:
        raise ValueError("Unable to split shape")

    return res


def smallest_dlon(shape):
    if not shape.is_empty and not hasattr(shape, '__iter__'):
        # only translate pure polygone
        dlon = shape.bounds[2] - shape.bounds[0]
        if dlon > 180:
            # logger.debug("large dlon %.0f converted  to short for shape %s" % (dlon,shape))
            return GeometryCollection(split_east_west(transform(lambda lon, lat: (lon % 360, lat), shape)))

    return shape


def transform_crs(shape, crs_in=crs_geographic, crs_out=crs_geographic):
    proj_in = pyproj.Proj(crs_in)
    proj_out = pyproj.Proj(crs_out)
    try:
        # pyproj >= 2.1
        project = pyproj.Transformer.from_proj(proj_in, proj_out, always_xy=True).transform
    except:
        logger.warning("old pyproj<2.1 will be deprecated")
        project = partial(
            pyproj.transform,
            proj_in,
            proj_out)

    transform_shape = transform(project, shape)
    if is_geographic(crs_out):
        transform_shape = transform_shape.smallest_dlon()
    if not transform_shape.is_valid:
        logger.warn('transform_crs is trying to correct invalid shape')

        if is_geographic(crs_out):
            # try lon_wrap
            transform_shape = GeometryCollection(split_east_west(
                transform(lambda x, y: pyproj.transform(pyproj.Proj(crs_in), pyproj.Proj(crs_out, lon_wrap=180), x, y),
                          shape)))
            for sub_shape in transform_shape:
                dlon = sub_shape.bounds[2] - sub_shape.bounds[0]
                if dlon > 180:
                    raise RuntimeError('dlon > 180 after split_east_west')
        if not transform_shape.is_valid:
            raise ValueError('transform_crs produced an invalid shape')

    return transform_shape


def get_aeqd_crs(shape, crs=crs_geographic):
    """
    return crs dict describing the azimuthal equistant projection centered on centroid's shape"""

    # only works with lat lon crs. Convert if needed
    if not is_geographic(crs):
        # only centroid en enought
        shape = transform_crs(shape.centroid, crs, crs_geographic)
    crs_aeqd = {
        #        'init' : "epsg:4326", # this line is needed for pyproj <=2 . It's a bug https://github.com/pyproj4/pyproj/issues/134
        'proj': 'aeqd',
        'lat_0': shape.centroid.y,
        'lon_0': shape.centroid.x,
        'x_0': 0,
        'y_0': 0,
        'ellps': 'WGS84'
    }
    if hasattr(pyproj, 'CRS'):
        crs_aeqd = pyproj.CRS(crs_aeqd)
    return crs_aeqd


def interp_line(shape, dist=1):
    """ return shape with added points every dist.
      shape mus be handled by shape.intepolate (ie line, not poly)
    """
    logging.warning("interp_line is an experimental feature")
    n = int(np.ceil((shape.length / dist)))
    return LineString([shape.interpolate(i / n, normalized=True) for i in range(0, n + 1)])


def interp_poly(shape, dist=1):
    """ call interp_line on every edges of a poly """
    logging.warning("interp_poly is an experimental feature")
    return Polygon(interp_line(shape.exterior, dist=dist))


def preserve_geography(shape, crs=crs_geographic, dist=None):
    """ given a shape in lon/lat coordinate, return an interpolated shape on great circle """
    logging.warning("preserve_geography is an experimental feature")
    aeqd_crs = get_aeqd_crs(shape)
    azi_shape = transform_crs(shape, crs, aeqd_crs)
    if dist is not None:
        azi_shape = interp_poly(azi_shape, dist)
    real_shape = transform_crs(azi_shape, aeqd_crs, crs)
    return real_shape


def buffer_meters(shape, dist, crs=crs_geographic):
    aeqd_crs = get_aeqd_crs(shape, crs)
    azi_shape = transform_crs(shape, crs, aeqd_crs)
    azi_buffer = azi_shape.buffer(dist)
    real_shape = transform_crs(azi_buffer, aeqd_crs, crs)
    return real_shape


def simplify_meters(shape, dist, crs=crs_geographic):
    aeqd_crs = get_aeqd_crs(shape, crs)
    azi_shape = transform_crs(shape, crs, aeqd_crs)
    azi_simpl = azi_shape.simplify(dist)
    real_shape = transform_crs(azi_simpl, aeqd_crs, crs)
    return real_shape


def distance_meters(s1, s2, crs=crs_geographic):
    aeqd_crs = get_aeqd_crs(GeometryCollection([s1, s2]), crs=crs)
    azi_s1 = transform_crs(s1, crs, aeqd_crs)
    azi_s2 = transform_crs(s2, crs, aeqd_crs)
    return azi_s1.distance(azi_s2)


# enrich OO shapely methods
for Geom in [GeometryCollection, MultiPolygon, Polygon, LineString, Point, MultiPoint]:
    Geom.buffer_meters = buffer_meters
    Geom.simplify_meters = simplify_meters
    Geom.smallest_dlon = smallest_dlon
    Geom.distance_meters = distance_meters
