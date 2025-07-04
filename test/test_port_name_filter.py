import re
from pipe_anchorages import port_name_filter


bad_examples = r"""
CITY
B 2 A
AN
(NULL)
@@@@@@@@@@@@@@@@@
?: :T/`9NKCL;KR=-[V
A\\]]:]\[\
VIGO>MOANA>VIGO
BDAM VIA NOK
GILLNETTER 47882224
OIL & GAS ACTIVITY
TEL:91679631
TEL 91679650
TLF +4791679632
TLF +4791679613
TEL 004748174725
CALL +4741626440
PHONE +47 916 79 642
48243996/95913043-
+47 91679625
TLF 974 38 884
PHONE:+47 916 79 626
PHONE 0046705808157
MOB 91679603 VHF 16
MOB:45028997
TLF 91679646
LOCAL TOWING
HOME
ZONA PESCA
AREA PESCA
PESCA
PESCA MEDITERRANEA
Z0NE DE PECHE
FISHING
FISHING GROUNDS
FISHING GROUND
FISHINGGROUND
FISHING BALIKAVLAMA
FISHING IN SKAGERAK
FISHING IN SKAGERACK
A, B, C, O,
ON SEA TRIAL
SEA TRIAL
SEA TRIAL(HHI-2468)
SEA TRIAL (H-4089)
SEA TRIAL-H HI2503
CEA TRIAL
CH 16 OR 95774104
VHF CH 16
VHF.CH.16
CH 16 FOR INFO
CH 16
CH 16 FOR DESTINATIO
>US SFO HARBOR DUTY
WORKING CHANNEL 14 .
SOUTH FLORIDA
><)))`>
??????
@@@@@@@@@@681-8436
0@@@T@E@@ @D@@@@JT
@@@@@@@@@ RESCUE
O??????? P?????????
M/Y SPACE OR SHORE
FISHFARMS
OWER BY NAVALMARINE
TLF.91679608 VHF.16
SAR IN OPERATION
SAR ON DUTY
SEARCH AND RESCUE
SEARCH & RESCUE
SAR OPS
POLICE
ON DUTY
A5B?3GD/<E
Y B@&Y$YY&@FY!;&X
50 58` N - 001 44` E
ON.THE.BAY
?, *, !
FOR ORDER
MALTA FOR ORDER
WORKING IN PORT
SURVEYING IN CHANNEL
DREDGE AREA
A OIL FIELD
PLY>PTOWN>PLY
SPLIT-DUBROVNIK-SPLI
00000O0
BBBBBBBBB
00000000,0*06]
BOC<=>OIL FIELDS
H@@@@^
0000,0*19]
ON.THE.BAY
SAIL IN PARADE
?, *, !
WORKING IN PORT
SURVEYING IN CHANNEL
DREDGE AREA
WORKING AREA
COASTAL TRADE
HARBOR WORK
JUST SAILING
NO DESTINATION
COASTAL TOWING
LOCAL CRUISING
CRUISING
CLASSIFIED
CANAL ESCORT
""".strip().split(
    "\n"
)

good_examples = r"""
 >JP SHN
JP UKB
JP KGA
>JP UNS OFF/E
>JP KNM C
MASINLOC
MANILA
TABACO
TAWAU
OLD MANGALOR IND
NEW MANGALOR
AU NTL
AU QDN
ST.PETERSBURG
NORFOLK
PALOMA
HONG KONG
HU LU DAO
FREEPORT,BAHAMA
>JP AKADOMARI OFF
PASIR GUDANG
NEWPORT OREGON
CARTAGENA
>KR BUSAN
MARINA DI RAGUSA
LIAN YUN GANG
PHUKET
EMDEN
BANYUWANGI
CN MJS
US RDB
""".strip().split(
    "\n"
)

punctuation = re.compile(r"[\W +]+")

number = re.compile(r"\d+")

invalid_prefixes = set(
    [
        "RESCUE",
        "FISHFARMS",
        "TLF ",
        "TEL ",
        "CALL ",
        "PHONE",
        "FISHING",
        "MOB ",
        "SAR ",
        "SEARCH AND RESCUE",
        "POLICE",
        "ON DUTY",
        "WORKING",
        "SURVEYING",
        "SEA TRIAL",
        "CH ",
        "VHF CH",
        "PESCA ",
        "DREDGE ",
    ]
)

invalid_suffixes = set(
    [
        "FOR ORDER",
        "TOWING",
        "CRUISING",
        "OIL FIELDS",
        "OIL FIELD",
        "DREDGE",
        "PARADE",
        "TRADE",
        "WORK",
        " TRIAL",
        "PESCA",
        "PECHE",
        "OIL FIELDS",
        "HARBOR DUTY",
        " ESCORT",
        "SAILING",
    ]
)


def test_is_valid_name():
    assert list(port_name_filter.normalized_valid_names(bad_examples)) == []
    assert list(port_name_filter.normalized_valid_names(good_examples)) == [
        port_name_filter.normalize(x) for x in good_examples
    ]
    assert list(port_name_filter.normalized_valid_names(["SHANG  HAI"])) == ["SHANG HAI"]


def test_normalize():
    assert port_name_filter.normalize("SHANG  HAI") == "SHANG HAI"
