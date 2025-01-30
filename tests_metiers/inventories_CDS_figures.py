import cartopy.crs as ccrs
import pandas as pd
import numpy as np
import datetime
import logging
import collections
from shapely.ops import unary_union
import cartopy.feature as cfeature
from matplotlib import pyplot as plt
from shapely.geometry import Point, Polygon
from dateutil import rrule


def map_footprints(geometry_request, collected_data_norm, title, alpha=0.8):
    """

    :param geometry_request (pd.Serie): for buoys location
    :param collected_data_norm (pd.DataFrame):  CDSE OData output where are the SAR footprints
    :param title (str):
    :return:
    """
    cpt = collections.defaultdict(int)
    fig = plt.figure(figsize=(15, 12), dpi=200)
    ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
    # ax.set_extent([160, 175, -26, -15])
    if isinstance(geometry_request[0], Point):
        for uu in geometry_request:
            plt.plot(*uu.xy, "r*", ms=3)
    else:
        for poly in geometry_request:
            if not isinstance(poly, Polygon):
                print("alerte", poly)
            plt.plot(*poly.exterior.xy, "r--", lw=2)
    ax.add_feature(cfeature.LAND)
    ax.add_feature(cfeature.COASTLINE)
    ax.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False)
    cpt_multipolygon = 0
    for uu in collected_data_norm["geometry"]:
        # print(uu)
        if uu.geom_type == "MultiPolygon":
            # print('multi polygon',uu)
            cpt_multipolygon += 1
            # dslmfkdmkfsl
            try:
                plt.plot(*unary_union(uu.geoms).exterior.xy)
            except ValueError:
                cpt["unary_union_not_sufficient"] += 1
                pass
                # print('unaryunion passe pas')
        elif uu.geom_type == "Polygon":
            plt.plot(*uu.exterior.xy, "b--", lw=0.7, alpha=alpha)
        else:
            print("strange geometry", uu)
    # plt.title('Sentinel-1 IW SLC available products %s since 2014'%(len(collected_data_norm['Name'])),fontsize=22)
    plt.title(title)
    plt.show()
    logging.info("counter: %s", cpt)
    print(
        "cpt_multipolygon", cpt_multipolygon, "/", len(collected_data_norm["geometry"])
    )


def histogram_ocean_coverage(collected_data_norm, title):
    plt.figure(dpi=100)
    plt.title(title)
    plt.hist(
        collected_data_norm["sea_percent"],
        20,
        label="CDS results total:%s" % len(collected_data_norm["sea_percent"]),
        edgecolor="k",
    )
    plt.xlabel("minimum % sea in footprint of slices found")
    plt.ylabel("number of slices found")
    plt.legend()
    plt.grid(True)
    plt.show()


def histogram_ocean_coverage_cumsum(collected_data_norm, title):
    # bins = np.arange(100,0,-5)
    valh, bins = np.histogram(collected_data_norm["sea_percent"], bins=20)
    cumsum = np.cumsum(valh[::-1])  # [::-1]
    bins2 = bins[::-1][1:]
    print("cumsum", cumsum)
    print("bins2", bins2)
    plt.figure(dpi=100)
    plt.title(title)
    plt.bar(
        bins2,
        cumsum,
        width=4,
        label="CDS results total:%s" % len(collected_data_norm["sea_percent"]),
        edgecolor="k",
        fc="y",
    )
    plt.xlabel("minimum % sea in footprint of slices found")
    plt.ylabel("cumulative number of slices found")
    plt.legend()
    plt.grid(True)
    plt.show()


def add_time_index_based_onstardtate(collected_data_norm):
    hm = []
    for uu in collected_data_norm["Name"].values:
        if isinstance(uu, str):
            hm.append(datetime.datetime.strptime(uu.split("_")[5], "%Y%m%dT%H%M%S"))
        else:
            hm.append(np.nan)
    # collected_data_norm["startdate"] = hm
    collected_data_norm.insert(0, "startdate", hm)
    collected_data_norm = collected_data_norm.set_index("startdate")

    return collected_data_norm


def add_orientation_pass_column(collected_data_norm):
    all_pass = []
    for kk in collected_data_norm["Attributes"]:
        for ll in kk:
            if ll["Name"] == "orbitDirection":
                all_pass.append(ll["Value"])
    # collected_data_norm["pass"] = all_pass
    collected_data_norm.insert(0, "pass", all_pass)
    return collected_data_norm


def number_product_per_month(collected_data_norm, title):
    collected_data_norm = add_time_index_based_onstardtate(collected_data_norm)
    cummul_grp = None
    freq = "M"
    ix = pd.date_range(
        start=datetime.datetime(2013, 1, 1),
        end=datetime.datetime(2024, 1, 1),
        freq=freq,
    )
    plt.figure(figsize=(15, 6), dpi=110)
    for unit in ["S1A", "S1B"]:
        for pol in ["1SDV", "1SSV", "1SSH", "1SDH"]:
            subset = collected_data_norm[
                (collected_data_norm["Name"].str.contains(unit))
                & (collected_data_norm["Name"].str.contains(pol))
            ]
            grp = subset.groupby(pd.Grouper(freq=freq)).count()
            grp = grp.reindex(ix)
            # if len(grp['Name'])>0:
            if grp["Name"].sum() > 0:
                # grp['Name'].plot(marker='.',label='%s %s'%(unit,pol),ms=12,markeredgecolor='k',lw=0.7)
                plt.bar(
                    grp.index,
                    grp["Name"],
                    width=30,
                    label="%s %s : %s products" % (unit, pol, len(subset)),
                    bottom=cummul_grp,
                    edgecolor="k",
                )
            # cumsum += grp['Name']
            if cummul_grp is None:
                cummul_grp = grp["Name"].fillna(0)
            else:
                # cummul_grp = pd.merge([cummul_grp,grp['Name']])
                # cummul_grp.merge(grp)
                cummul_grp += grp["Name"].fillna(0)
    plt.legend(fontsize=15, loc=2)
    plt.grid(True)
    plt.title(title, fontsize=18)
    plt.yticks(fontsize=18)
    plt.xticks(fontsize=18, rotation=45)
    plt.xlim(ix[0], ix[-1])
    plt.ylabel("Number of IW SLC products available\nstacked histogram", fontsize=17)
    plt.show()


def number_of_product_per_climato_month(collected_data_norm, title):
    if "startdate" not in collected_data_norm:
        collected_data_norm = add_time_index_based_onstardtate(collected_data_norm)
    plt.figure(figsize=(13, 6), dpi=110)
    for unit in ["S1A", "S1B"]:
        for pol in ["1SDV", "1SSV", "1SSH", "1SDH"]:
            subset = collected_data_norm[
                (collected_data_norm["Name"].str.contains(unit))
                & (collected_data_norm["Name"].str.contains(pol))
            ]
            # grp = subset.groupby(subset.index.month).count()
            grp = subset.groupby(subset["startdate"].dt.month).count()
            # print('grp',grp)
            if len(grp["Name"]) > 0:
                grp["Name"].plot(
                    marker=".",
                    label="%s %s" % (unit, pol),
                    ms=12,
                    markeredgecolor="k",
                    lw=0.7,
                )
    plt.legend(fontsize=15, bbox_to_anchor=(1, 1))
    plt.grid(True)
    plt.title(title, fontsize=18)
    plt.yticks(fontsize=18)
    plt.xticks(
        np.arange(1, 13),
        [
            "Jan",
            "Fev",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dev",
        ],
        fontsize=12,
    )
    plt.xlabel("month of the year", fontsize=18)
    plt.ylabel("Number of product", fontsize=18)
    plt.show()


def number_of_product_per_year_asc_desc(collected_data_norm, title):
    collected_data_norm = add_time_index_based_onstardtate(collected_data_norm)
    collected_data_norm = add_orientation_pass_column(collected_data_norm)
    plt.figure(figsize=(10, 6), dpi=110)
    cumsum = None
    cummul_grp = None
    freq = "AS"
    colors = ["brown", "grey", "cyan", "magenta"]
    ix = pd.date_range(
        start=datetime.datetime(2013, 1, 1),
        end=datetime.datetime(2024, 1, 1),
        freq=freq,
    )

    cptu = 0
    for unit in ["S1A", "S1B"]:
        for passe in ["ASCENDING", "DESCENDING"]:
            subset = collected_data_norm[
                (collected_data_norm["Name"].str.contains(unit))
                & (collected_data_norm["pass"].str.contains(passe))
            ]
            grp = subset.groupby(pd.Grouper(freq=freq)).count()
            grp = grp.reindex(ix)
            if cumsum is None:
                cumsum = np.zeros(grp["Name"].size)

            if len(grp["Name"]) > 0:
                # grp['Name'].plot(marker='.',label='%s %s'%(unit,passe),ms=12,markeredgecolor='k',lw=0.7)
                plt.bar(
                    grp.index,
                    grp["Name"],
                    width=300,
                    label="%s %s: %s products" % (unit, passe, len(subset)),
                    bottom=cummul_grp,
                    edgecolor="k",
                    fc=colors[cptu],
                )
            else:
                print("riend ans ce group")
            cumsum += grp["Name"]
            if cummul_grp is None:
                cummul_grp = grp["Name"].fillna(0)
            else:
                # cummul_grp = pd.merge([cummul_grp,grp['Name']])
                # cummul_grp.merge(grp)
                cummul_grp += grp["Name"].fillna(0)
            # print('cummul_grp',cummul_grp)
            cptu += 1
    plt.legend(fontsize=12, bbox_to_anchor=(1, 1))
    plt.grid(True)
    plt.title(title, fontsize=18)
    plt.yticks(fontsize=12)
    plt.xticks(fontsize=12)
    plt.ylabel("Number of IW SLC products available\nstacked histogram", fontsize=15)
    plt.show()


def add_volumetry_column(collected_data_norm):
    """
    for IW it i s True
    :param collected_data_norm:
    :return:
    """
    vols = []
    for kk in collected_data_norm["Name"]:
        if "EW" in kk and "OCN" in kk:
            vols.append(37.0 / 1000.0)
        elif "EW" in kk and "SLC" in kk:
            vols.append(3.7 / 1.0)
        elif "IW" in kk and "OCN" in kk:
            vols.append(15.0 / 1000.0)
        else:
            vols.append(3.8 / 1000.0)
        # if "EW" in kk or "WV" in kk:
        #     raise Exception("mode no configured")
        # if "1SDV" in kk or "1SDH" in kk:
        #     vols.append(7.8 / 1000.0)
        # else:
        #     vols.append(3.8 / 1000.0)
    # collected_data_norm["volume"] = vols
    collected_data_norm.insert(0, "volume", vols)
    return collected_data_norm


def volume_per_year(collected_data_norm, title, freq="AS"):
    """

    :param collected_data_norm:
    :param title:
    :param freq: AS is for yearly grouping with anchor at the start of the year
    :return:
    """
    collected_data_norm = add_volumetry_column(collected_data_norm)
    collected_data_norm = add_time_index_based_onstardtate(collected_data_norm)
    plt.figure(figsize=(10, 6), dpi=110)
    cummul_grp = None
    # not Y because anchored date is offset to year+1
    # freq = "M"  # for a test
    if freq == "AS":
        width = 300
    elif freq == "M":
        width = 30

    ix = pd.date_range(
        start=datetime.datetime(2013, 1, 1),
        end=datetime.datetime(2024, 1, 1),
        freq=freq,
    )
    cptu = 0
    for unit in ["S1A", "S1B"]:
        for pol in ["1SDV", "1SSV", "1SSH", "1SDH"]:
            subset = collected_data_norm[
                (collected_data_norm["Name"].str.contains(unit))
                & (collected_data_norm["Name"].str.contains(pol))
            ]
            subset = subset["volume"]
            grp = subset.groupby(pd.Grouper(freq=freq)).sum()
            grp = grp.reindex(ix)
            if grp.sum() > 0:
                plt.bar(
                    grp.index,
                    grp,
                    width=width,
                    label="%s %s: %s products volume: %1.1f To"
                    % (unit, pol, len(subset), subset.sum()),
                    bottom=cummul_grp,
                    edgecolor="k",
                )
            else:
                print("riend ans ce group")
            if cummul_grp is None:
                cummul_grp = grp.fillna(0)
            else:
                cummul_grp += grp.fillna(0)
            cptu += 1
    plt.legend(fontsize=10, loc=2)
    plt.grid(True)
    plt.title(title, fontsize=18)
    plt.yticks(fontsize=12)
    plt.xticks(fontsize=12)
    plt.ylabel(
        "Volume of IW SLC products available [GigaOctet]\nstacked histogram",
        fontsize=15,
    )
    plt.show()


def count_per_year_with_labels(collected_data_norm, title, freq="AS"):
    """

    :param collected_data_norm:
    :param title:
    :param freq: AS is for yearly grouping with anchor at the start of the year
    :return:
    """
    if "volume" not in collected_data_norm:
        collected_data_norm = add_volumetry_column(collected_data_norm)
    if "startdate" not in collected_data_norm:
        collected_data_norm = add_time_index_based_onstardtate(collected_data_norm)
    plt.figure(figsize=(10, 6), dpi=110)
    # not Y because anchored date is offset to year+1

    newdf_per_class_double_entries = {}
    years = []
    newdf_per_class_double_entries["1SDV"] = []
    newdf_per_class_double_entries["1SSV"] = []
    newdf_per_class_double_entries["1SSH"] = []
    newdf_per_class_double_entries["1SDH"] = []
    for year in range(2014, 2024):
        years.append(year)
        for pol in ["1SDV", "1SSV", "1SSH", "1SDH"]:
            subset = collected_data_norm[
                (
                    collected_data_norm["Name"].str.contains(pol + "_" + str(year))
                )  # & (collected_data_norm["Name"].str.contains(pol))
            ]
            # print(subset)
            # subset = subset["volume"]
            countsafe = subset["Name"].count()
            # grp = subset.groupby(pd.Grouper(freq=freq)).sum()
            # grp = grp.reindex(ix)
            # if countsafe > 0:

            newdf_per_class_double_entries["%s" % pol].append(countsafe)
            # newdf_per_class_double_entries["pola"] = pol
    # print("dict ", newdf_per_class_double_entries)
    newdf = pd.DataFrame(newdf_per_class_double_entries, index=years)
    # print("newdf", newdf)
    ax = newdf.plot(
        kind="bar", stacked=True, figsize=(8, 6), rot=0, xlabel="year", ylabel="Count"
    )
    for c in ax.containers:

        # Optional: if the segment is small or 0, customize the labels
        labels = [int(v.get_height()) if v.get_height() > 0 else "" for v in c]

        # remove the labels parameter if it's not needed for customized labels
        ax.bar_label(c, labels=labels, label_type="center")
    plt.legend(fontsize=10, loc=2)
    plt.grid(True)
    plt.title(title, fontsize=18)
    plt.yticks(fontsize=12)
    plt.xticks(fontsize=12)
    plt.ylabel(
        "Count IW SLC products available \nstacked histogram",
        fontsize=15,
    )
    plt.show()


def count_per_year_with_labels_unit(
    collected_data_norm,
    title,
    freq="AS",
    yearmin=2013,
    yearmax=2024,
    addlegendonlyifcountnotnull=True,
):
    """

    :param collected_data_norm:
    :param title:
    :param freq: AS is for yearly grouping with anchor at the start of the year
    :return:
    """
    if "volume" not in collected_data_norm:
        collected_data_norm = add_volumetry_column(collected_data_norm)
    if "startdate" not in collected_data_norm:
        collected_data_norm = add_time_index_based_onstardtate(collected_data_norm)
    plt.figure(figsize=(10, 6), dpi=110)
    # not Y because anchored date is offset to year+1
    newdf_per_class_double_entries = {}
    years = []
    for year in range(yearmin, yearmax + 1):
        years.append(year)
        for sarunit in ["S1A", "S1B"]:
            for pol in ["1SDV", "1SSV", "1SSH", "1SDH"]:

                subset = collected_data_norm[
                    (collected_data_norm["Name"].str.contains(pol + "_" + str(year)))
                    & (collected_data_norm["Name"].str.contains(sarunit))  #
                ]
                # print(subset)
                # subset = subset["volume"]
                countsafe = subset["Name"].count()
                # grp = subset.groupby(pd.Grouper(freq=freq)).sum()
                # grp = grp.reindex(ix)
                # if countsafe > 0:
                if addlegendonlyifcountnotnull:
                    if countsafe > 0:
                        if sarunit + "_" + pol not in newdf_per_class_double_entries:
                            newdf_per_class_double_entries[
                                "%s" % (sarunit + "_" + pol)
                            ] = []
                        newdf_per_class_double_entries[
                            "%s" % (sarunit + "_" + pol)
                        ].append(countsafe)
                else:
                    if sarunit + "_" + pol not in newdf_per_class_double_entries:
                        newdf_per_class_double_entries["%s" % (sarunit + "_" + pol)] = (
                            []
                        )
                    newdf_per_class_double_entries["%s" % (sarunit + "_" + pol)].append(
                        countsafe
                    )
                # newdf_per_class_double_entries["pola"] = pol
    # print("dict ", newdf_per_class_double_entries)
    newdf = pd.DataFrame(newdf_per_class_double_entries, index=years)
    # print("newdf", newdf)
    ax = newdf.plot(
        kind="bar",
        stacked=True,
        figsize=(8, 6),
        rot=0,
        xlabel="year",
        ylabel="Count",
        edgecolor="k",
    )
    for c in ax.containers:

        # Optional: if the segment is small or 0, customize the labels
        labels = [int(v.get_height()) if v.get_height() > 0 else "" for v in c]

        # remove the labels parameter if it's not needed for customized labels
        ax.bar_label(c, labels=labels, label_type="center")
    plt.legend(fontsize=10, ncols=4, bbox_to_anchor=(1, -0.1))
    plt.grid(True)
    plt.title(title, fontsize=18)
    plt.yticks(fontsize=12)
    plt.xticks(fontsize=12)
    plt.ylabel(
        "Count SAFE products available on CDSE \nstacked histogram",
        fontsize=15,
    )
    plt.show()


def volume_wrt_sea_percent(collected_data_norm, title):
    collected_data_norm = add_volumetry_column(collected_data_norm)
    delta = 10
    sea_perc = np.arange(0, 100, 10)

    plt.figure(dpi=120)
    cummul_grp = np.zeros(len(sea_perc))
    for pol in ["1SDV", "1SSV", "1SSH", "1SDH"]:
        total_volumes = []
        subset = collected_data_norm[(collected_data_norm["Name"].str.contains(pol))]
        for seap in sea_perc:
            subset2 = subset[subset["sea_percent"] >= seap]
            total_volumes.append(subset2["volume"].sum())

        total_volumes = np.array(total_volumes)
        if (total_volumes > 0).any():
            plt.bar(
                sea_perc,
                total_volumes,
                width=delta,
                edgecolor="k",
                align="edge",
                label=pol,
                bottom=cummul_grp,
            )
            cummul_grp += total_volumes

    plt.title(title)

    plt.xlabel("minimum % of ocean in the footprint")
    plt.ylabel("total volume of the S-1 product considered [To]")
    plt.grid(True)
    plt.legend()
    plt.show()


def count_per_year_with_labels_available(
    collected_data_norm,
    title,
    freq="AS",
    yearmin=2013,
    yearmax=2024,
    addlegendonlyifcountnotnull=True,
):
    """

    :param collected_data_norm:
    :param title:
    :param freq: AS is for yearly grouping with anchor at the start of the year
    :return:
    """
    import matplotlib.patheffects as pe

    assert "available@Ifremer" in collected_data_norm
    collected_data_norm = add_volumetry_column(collected_data_norm)
    collected_data_norm = add_time_index_based_onstardtate(collected_data_norm)
    fig = plt.figure(figsize=(10, 6), dpi=110)
    ax = plt.subplot(111)
    # not Y because anchored date is offset to year+1
    newdf_per_class_double_entries = {}

    for mode in ["all", "available@Ifremer"]:
        years = []
        for year in range(yearmin, yearmax + 1):
            years.append(year)
            if mode == "available@Ifremer":
                subset = collected_data_norm[
                    (collected_data_norm["available@Ifremer"])
                    & (collected_data_norm["Name"].str.contains("_" + str(year)))
                ]
            else:
                subset = collected_data_norm[
                    (collected_data_norm["Name"].str.contains("_" + str(year)))
                ]

            countsafe = subset["Name"].count()
            if addlegendonlyifcountnotnull:
                if countsafe > 0:
                    if mode not in newdf_per_class_double_entries:
                        newdf_per_class_double_entries["%s" % mode] = []
                    newdf_per_class_double_entries["%s" % mode].append(countsafe)
            else:
                if mode not in newdf_per_class_double_entries:
                    newdf_per_class_double_entries["%s" % mode] = []
                newdf_per_class_double_entries["%s" % mode].append(countsafe)
        newdf = pd.DataFrame(newdf_per_class_double_entries, index=years)
        # print("newdf", newdf)
    ax = newdf.plot(
        kind="bar",
        stacked=False,
        figsize=(8, 6),
        rot=0,
        xlabel="year",
        ylabel="Count",
        edgecolor="k",
        ax=ax,
    )
    for c in ax.containers:

        # Optional: if the segment is small or 0, customize the labels
        labels = [int(v.get_height()) if v.get_height() > 0 else "" for v in c]

        # remove the labels parameter if it's not needed for customized labels
        ax.bar_label(
            c,
            labels=labels,
            label_type="center",
            fontsize=10,
            path_effects=[pe.withStroke(linewidth=4, foreground="white")],
        )
    plt.legend(fontsize=10, ncols=4, bbox_to_anchor=(1, -0.1))
    plt.grid(True)
    plt.title(title, fontsize=18)
    plt.yticks(fontsize=12)
    plt.xticks(fontsize=12)
    plt.ylabel(
        "Count SAFE products",
        fontsize=15,
    )
    plt.show()
    return fig


def count_per_month_with_labels_unit(
    collected_data_norm,
    title,
    freq="AS",
    monthmin=201301,
    monthmax=202412,
    addlegendonlyifcountnotnull=True,
):
    """

    :param collected_data_norm:
    :param title:
    :param freq: AS is for yearly grouping with anchor at the start of the year
    :return:
    """
    if "volume" not in collected_data_norm:
        collected_data_norm = add_volumetry_column(collected_data_norm)
    if "startdate" not in collected_data_norm:
        collected_data_norm = add_time_index_based_onstardtate(collected_data_norm)
    plt.figure(figsize=(10, 6), dpi=110)
    # not Y because anchored date is offset to year+1
    # freq = "M"  # for a test

    width = 30
    newdf_per_class_double_entries = {}
    months = []
    months_str = []
    # for year in range(yearmin, yearmax + 1):
    for month in rrule.rrule(
        rrule.MONTHLY,
        dtstart=datetime.datetime.strptime(monthmin, "%Y%m"),
        until=datetime.datetime.strptime(monthmax, "%Y%m"),
    ):
        months.append(month)
        months_str.append(month.strftime("%Y%b"))
        for sarunit in ["S1A", "S1B"]:
            for pol in ["1SDV", "1SSV", "1SSH", "1SDH"]:

                subset = collected_data_norm[
                    (collected_data_norm["Name"].str.contains(pol + "_"))
                    & (collected_data_norm["startdate"] >= month)
                    & (
                        collected_data_norm["startdate"]
                        < month + datetime.timedelta(days=width)
                    )
                    & (collected_data_norm["Name"].str.contains(sarunit))  #
                ]
                # print(subset)
                # subset = subset["volume"]
                countsafe = subset["Name"].count()
                # grp = subset.groupby(pd.Grouper(freq=freq)).sum()
                # grp = grp.reindex(ix)
                # if countsafe > 0:
                if addlegendonlyifcountnotnull:
                    if countsafe > 0:
                        if sarunit + "_" + pol not in newdf_per_class_double_entries:
                            newdf_per_class_double_entries[
                                "%s" % (sarunit + "_" + pol)
                            ] = []
                        newdf_per_class_double_entries[
                            "%s" % (sarunit + "_" + pol)
                        ].append(countsafe)
                else:
                    if sarunit + "_" + pol not in newdf_per_class_double_entries:
                        newdf_per_class_double_entries["%s" % (sarunit + "_" + pol)] = (
                            []
                        )
                    newdf_per_class_double_entries["%s" % (sarunit + "_" + pol)].append(
                        countsafe
                    )
                # newdf_per_class_double_entries["pola"] = pol
    print("dict ", newdf_per_class_double_entries)
    newdf = pd.DataFrame(newdf_per_class_double_entries, index=months)
    # print("newdf", newdf)
    ax = newdf.plot(
        kind="bar",
        stacked=True,
        figsize=(8, 6),
        rot=0,
        xlabel="year",
        ylabel="Count",
        edgecolor="k",
    )
    for c in ax.containers:

        # Optional: if the segment is small or 0, customize the labels
        labels = [int(v.get_height()) if v.get_height() > 0 else "" for v in c]

        # remove the labels parameter if it's not needed for customized labels
        ax.bar_label(c, labels=labels, label_type="center")
    # plt.legend(fontsize=10, ncols=4, bbox_to_anchor=(1, -0.1))
    plt.legend(fontsize=10, ncols=2, bbox_to_anchor=(1, 1))
    plt.grid(True)
    plt.title(title, fontsize=18)
    plt.yticks(fontsize=12)
    ax = plt.gca()
    ti = ax.get_xticks()
    plt.xticks(ticks=ti, labels=months_str, fontsize=12, rotation=45)
    plt.ylabel(
        "Count SAFE products available on CDSE \nstacked histogram",
        fontsize=15,
    )
    plt.show()
