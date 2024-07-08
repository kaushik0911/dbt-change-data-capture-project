{{
    config(
        materialized='incremental',
        unique_key='summons_number',
    ) 
}}

SELECT
    `Summons Number` AS summons_number,
    `Plate ID` AS plate_id,
    `Registration State` AS registration_state,
    `Plate Type` AS plate_type,
    `Violation Code` AS violation_code,
    `Vehicle Body Type` AS vehicle_body_type,
    `Vehicle Make` AS vehicle_make,
    `Issuing Agency` AS issuing_agency,
    `Street Code1` AS street_code1,
    `Street Code2` AS street_code2,
    `Street Code3` AS street_code3,
    `Violation Location` AS violation_location,
    `Violation Precinct` AS violation_precinct,
    `Issuer Precinct` AS issuer_precinct,
    `Issuer Code` AS issuer_code,
    `Issuer Command` AS issuer_command,
    `Issuer Squad` AS issuer_squad,
    `Violation Time` AS violation_time,
    `Time First Observed` AS time_first_observed,
    `Violation County` AS violation_county,
    `Violation In Front Of Or Opposite` AS violation_in_front_of_or_opposite,
    `House Number` AS house_number,
    `Street Name` AS street_name,
    `Intersecting Street` AS intersecting_street,
    `Law Section` AS law_section,
    `Sub Division` AS sub_division,
    `Violation Legal Code` AS violation_legal_code,
    `Days Parking In Effect` AS days_parking_in_effect,
    `From Hours In Effect` AS from_hours_in_effect,
    `To Hours In Effect` AS to_hours_in_effect,
    `Vehicle Color` AS vehicle_color,
    `Unregistered Vehicle?` AS unregistered_vehicle,
    `Vehicle Year` AS vehicle_year,
    `Meter Number` AS meter_number,
    `Feet From Curb` AS feet_from_curb,
    `Violation Post Code` AS violation_post_code,
    `Violation Description` AS violation_description,
    {{ integer_like_string_date_to_date('Vehicle Expiration Date') }} AS vehicle_expiration_date,
    {{ integer_like_string_date_to_date('Date First Observed') }} AS date_first_observed
FROM
    {{ref('parking_violations_raw')}}