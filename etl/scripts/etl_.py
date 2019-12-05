# -*- coding: utf-8 -*-

import os.path as osp

import pandas as pd
import requests as req
from io import BytesIO

from ddf_utils.str import to_concept_id
from ddf_utils.dsl import *

source = "../source/unhcr_popstats_export_time_series_all_data.csv"
synonyms = "https://raw.githubusercontent.com/open-numbers/ddf--open_numbers/master/ddf--synonyms--geo.csv"

out_dir = '../../'

def _conv(s):
    if s == 'Refugees (incl. refugee-like situations)':
        return 'refugees'
    else:
        return to_concept_id(s)

def main():
    data = pd.read_csv(source, encoding='latin', skiprows=3, na_values=['*'])

    # get all countries. we treat origin and country column as same
    seta = set(data['Country / territory of asylum/residence'].unique())
    setb = set(data['Origin'].unique())

    all_countries = setb.union(seta)

    geo_syn = pd.read_csv(BytesIO(req.get(synonyms).content))
    syndict = geo_syn.set_index('synonym')['geo'].to_dict()

    # hard coding some of them
    other_dict = {
        'Serbia and Kosovo (S/RES/1244 (1999))': 'srb',
        'Bonaire': 'bonaire',
        'Stateless': 'stateless',
        'Various/Unknown': 'various_unknown',
        'Tibetan': 'tibetan'
    }

    countrymapping = dict()
    for c in all_countries:
        c_ = c.strip()
        m = syndict.get(c_, None)
        if m is None:
            m = other_dict[c_]

        countrymapping[c_] = m

    # displacment type
    pop_type = data['Population type'].unique()

    popmap = dict((p, _conv(p)) for p in pop_type)

    # now convert the data to datapoints
    data.columns = ['year', 'asylum_residence', 'origin', 'displacement_type', 'displaced_population']

    # temporary fix for duplicated data. see #1
    # FIXME: remove the drop_duplicates call.
    data = data.drop_duplicates(subset=['asylum_residence', 'origin', 'displacement_type', 'year'])
    assert not has_duplicates(data, ['year', 'asylum_residence', 'origin', 'displacement_type'])

    data['asylum_residence'] = data['asylum_residence'].map(countrymapping)
    assert not data['asylum_residence'].hasnans

    data['origin'] = data['origin'].map(lambda x: countrymapping[x.strip()])
    assert not data['origin'].hasnans

    data['displacement_type'] = data['displacement_type'].map(popmap)
    data = data[['asylum_residence', 'origin', 'displacement_type', 'year', 'displaced_population']]
    data = data.sort_values(by=['asylum_residence', 'origin', 'displacement_type', 'year'])
    data['displaced_population'] = data['displaced_population'].replace("*", 3)
    data = data.dropna(subset=['displaced_population'])
    data.to_csv(osp.join(out_dir,
                         'ddf--datapoints--displaced_population--by--asylum_residence'
                         '--origin--displacement_type--year.csv'),
                index=False)

    # aggregate on the origin column.
    data2 = data.groupby(['asylum_residence', 'displacement_type', 'year'])['displaced_population'].agg(sum)
    data2.reset_index().to_csv(osp.join(out_dir,
                                        'ddf--datapoints--displaced_population--by--'
                                        'asylum_residence--displacement_type--year.csv'),
                               index=False)

    # entities
    countrydf = pd.DataFrame.from_dict(orient='index', data=countrymapping).reset_index()
    countrydf.columns = ['name', 'asylum_residence']
    countrydf = countrydf[['asylum_residence', 'name']]
    countrydf.to_csv('../../ddf--entities--asylum_residence.csv', index=False)

    countrydf.columns = ['origin', 'name']
    countrydf.to_csv('../../ddf--entities--origin.csv', index=False)

    poptypedf = pd.DataFrame.from_dict(popmap, orient='index').reset_index()
    poptypedf.columns = ['name', 'displacement_type']
    poptypedf = poptypedf[['displacement_type', 'name']]

    poptypedf.to_csv(osp.join(out_dir,
                              'ddf--entities--displacement_type.csv'), index=False)

    # concepts
    concepts = ['asylum_residence', 'origin', 'displacement_type', 'displaced_population', 'year', 'name', 'domain']
    cname = ['Country / territory of asylum/residence',
         'Origin', 'Displacement Type', 'Displaced Population', 'Year', 'Name', 'Domain']
    ctype = ['entity_domain', 'entity_domain', 'entity_domain', 'measure', 'time', 'string', 'string']
    # cdomain = None
    cdf = pd.DataFrame({'concept': concepts, 'name': cname, 'concept_type': ctype})
    cdf.to_csv('../../ddf--concepts.csv', index=False)

    print('Done!')


if __name__ == '__main__':
    main()
