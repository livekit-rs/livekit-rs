// Derived from golang.org/x/text v0.42.0 internal/language tables.
// Copyright The Go Authors. BSD-3-Clause, see SOURCE.json.
// Same two-letter ParseRegion/String/IsCountry intersection; includes historic
// CLDR country identifiers, not a hand-written ISO-only replacement.
pub(super) fn valid(value: &str) -> bool {
    const COUNTRIES: &[&str] = &[
        "AC", "AD", "AE", "AF", "AG", "AI", "AL", "AM", "AN", "AO", "AQ", "AR", "AS", "AT", "AU",
        "AW", "AX", "AZ", "BA", "BB", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BL", "BM", "BN",
        "BO", "BQ", "BR", "BS", "BT", "BU", "BV", "BW", "BY", "BZ", "CA", "CC", "CD", "CF", "CG",
        "CH", "CI", "CK", "CL", "CM", "CN", "CO", "CP", "CQ", "CR", "CS", "CT", "CU", "CV", "CW",
        "CX", "CY", "CZ", "DD", "DE", "DG", "DJ", "DK", "DM", "DO", "DY", "DZ", "EA", "EC", "EE",
        "EG", "EH", "ER", "ES", "ET", "EZ", "FI", "FJ", "FK", "FM", "FO", "FQ", "FR", "FX", "GA",
        "GB", "GD", "GE", "GF", "GG", "GH", "GI", "GL", "GM", "GN", "GP", "GQ", "GR", "GS", "GT",
        "GU", "GW", "GY", "HK", "HM", "HN", "HR", "HT", "HU", "HV", "IC", "ID", "IE", "IL", "IM",
        "IN", "IO", "IQ", "IR", "IS", "IT", "JE", "JM", "JO", "JP", "JT", "KE", "KG", "KH", "KI",
        "KM", "KN", "KP", "KR", "KW", "KY", "KZ", "LA", "LB", "LC", "LI", "LK", "LR", "LS", "LT",
        "LU", "LV", "LY", "MA", "MC", "MD", "ME", "MF", "MG", "MH", "MI", "MK", "ML", "MM", "MN",
        "MO", "MP", "MQ", "MR", "MS", "MT", "MU", "MV", "MW", "MX", "MY", "MZ", "NA", "NC", "NE",
        "NF", "NG", "NH", "NI", "NL", "NO", "NP", "NQ", "NR", "NT", "NU", "NZ", "OM", "PA", "PC",
        "PE", "PF", "PG", "PH", "PK", "PL", "PM", "PN", "PR", "PS", "PT", "PU", "PW", "PY", "PZ",
        "QA", "RE", "RH", "RO", "RS", "RU", "RW", "SA", "SB", "SC", "SD", "SE", "SG", "SH", "SI",
        "SJ", "SK", "SL", "SM", "SN", "SO", "SR", "SS", "ST", "SU", "SV", "SX", "SY", "SZ", "TA",
        "TC", "TD", "TF", "TG", "TH", "TJ", "TK", "TL", "TM", "TN", "TO", "TP", "TR", "TT", "TV",
        "TW", "TZ", "UA", "UG", "UK", "UM", "UN", "US", "UY", "UZ", "VA", "VC", "VD", "VE", "VG",
        "VI", "VN", "VU", "WF", "WK", "WS", "XK", "YD", "YE", "YT", "YU", "ZA", "ZM", "ZR", "ZW",
    ];
    COUNTRIES.binary_search(&value).is_ok()
}
