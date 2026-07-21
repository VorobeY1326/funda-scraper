POSTCODES_TO_NEIGHBOURHOODS = {
    (1101, 1115): 'Diemen',
    (1032, 1036): 'Noord',
}

def get_neighbourhood_or_city_name(city, postcode):
    if not postcode:
        return city
    try:
        postcode_digits = int(postcode[0:4])
    except Exception:
        return city
    for key in POSTCODES_TO_NEIGHBOURHOODS.keys():
        if key[0] <= postcode_digits <= key[1]:
            return POSTCODES_TO_NEIGHBOURHOODS[key]
    return city
