# Lemongrass
This repo is where I keep the tools I use to work on my 24 Hours of Lemons team.

# Telem

# How to Use
1. Set up an influxDB instance.
![Image of InfluxDB starting in log](images/influx.png)

```
> create user tom_admin with password 'THIS_IS_A_SECRET_TWAT' WITH ALL PRIVILEGES
> SHOW USERS
user		admin
----		-----
tom_admin	true

> CREATE USER car_252 WITH PASSWORD 'HEY_I_SAID_NO_PEEKING'
> SHOW USERS
user		admin
----		-----
tom_admin	true
car_252		false

> show databases
name: databases
name
----
_internal
stats_252

> grant write on stats_252 to car_252
```

2. Plug in a USB OBD Scanner and point telem at that instance.
```
client = InfluxDBClient('comms.wotlemons.com', 8086, 'car_252', 'HEY_I_SAID_NO_PEEKING', 'stats_252')
```

3. Git goin. [A feed from an ECUSim 2000 is available here.](https://stats.wotlemons.com/d/Heg64fsZk/ecusim-telemetry?orgId=1&refresh=5s&from=1580083825550&to=1580084125550)





# Laps

# How to Use
1. Get a Race Monitor API token
https://www.race-monitor.com/Home/API

2. Put your token in a dotfile named token in the root of the project folder

3. Get your race ID

We need a Race ID to get information for. Head to https://www.race-monitor.com/Live/Race while your race is live to get this easily from the URL.

![Image of Race ID in URL bar](https://i.imgur.com/1FQNvSb.png)

3. Run the tool

Requirements.txt coming maybe someday.

Usage:
```
./laps.py RACE_ID CAR_NUMBER
```

```
23:46 $ ./laps.py 87529 252
--------------------------------------------------------------------------------
Pos. #    First Name                       Laps Competitor ID   Transponder ID
--------------------------------------------------------------------------------
1    888  Mome Rath Racing                 514  49990521        9414531
2    43   3 Pedal Mafia                    508  49990522        637490
3    10   Tyre Festival                    506  49990523        6648066
4    71   3 Pedal Mafia                    498  49990524        5409914
5    945  The Ruptured Duck Racing         491  49990525        4-10
6    77   The MazdaRati                    490  49990526        3-19
7    213  SiSo Motorsports                 488  49990527        1-9
8    308  moustache ride                   486  49990528        1-18
9    241  Banksy                           486  49990529        1-12
10   125  Gadget Inspectors                473  49990530        1-4
11   82   B.A.R.F Motorsports              471  49990531        1548772
12   59   Low-T  Sewing Circle & Book Club 470  49990532        747653
13   482  Dead Horse Beaters               466  49990533        8012270
14   114  2-Broke 2-Care                   466  49990534        3871222
15   495  All Rhodes Racing                463  49990535        5682943
16   969  C-Team                           461  49990536        4-11
17   3    Lemontarians                     460  49990537        4-18
18   510  Bright Ideas Racing              457  49990538        1042252
19   32   Great Globs of Oil //Bratva      452  49990539        1510569
20   60   Team Napa know it all's          451  49990540        3-14
21   101  Garage Heroes (in training)      449  49990541        9565891
22   15   Nuthin' but a Z Thang            448  49990542        238308
23   55   Lemontarians                     446  49990543        3-13
24   847  Squidrope Racing                 443  49990544        4-6
25   314  The Science Project              439  49990546        1-19
25   345  Big Guy Racing                   438  49990545        4599295
27   19   Team Sputnik                     438  49990547        7750369
28   84   Altimate Warrior Racing          437  49990548        3-21
29   496  Moot Point Racing                431  49990549        1-30
30   17   FarFrömNascär                    429  49990550        30890
31   431  Rescue Auto Racing               428  49990551        1-27
32   295  WeLikeTheTunaHere                425  49990552        1-16
33   245  One Tire Fire                    424  49990553        1-13
34   91   Dispensables                     424  49990554        3-23
35   337  FireBirdBox                      415  49990555        1-22
36   117  Team TruckStuff                  413  49990556        1-3
37   47   We Audi be Faster I (WABF I)     405  49990557        1042046
38   76   Team Del Sol Train               401  49990558        3-18
39   31   Great Globs of Oil //Bratva      397  49990559        1519918
40   99   FRS's Ugly Uncle                 396  49990560        3-27
41   69   The Rowdy Rednecks               384  49990561        3-16
42   41   3 Pedal Mafia                    384  49990562        9320976
43   455  The Awkward Corner               383  49990563        1-28
44   88   88 Shades of Gray                383  49990564        3-22
45   7    AngryPork Racing                 383  49990565        1614039
46   4    Flame Car Racing                 379  49990566        3-3
47   21   Talladega Nights                 375  49990567        3-5
48   801  No Problemo!                     371  49990568        4-4
49   992  SillyNannies2                    371  49990569        4-13
50   50   Diners Drivers and Divebombs     368  49990570        3-12
51   1    Team Duty                        367  49990571        1957371
52   79   Hopes & Dreams Racing            366  49990572        3-20
53   36   Sabrina Duncan's Re-Run (-SDR-)  365  49990573        3-8
54   292  SillyNannies3                    352  49990574        1-15
55   182  lol NSA                          346  49990575        1-8
56   160  Scotch rocket                    344  49990576        1-7
57   103  Team Mean and the Mechanics      327  49990577        3-30
58   132  3 Pedal Mafia                    325  49990578        9679811
59   316  Hit 'Em With the Hein Racing     324  49990579        1-21
60   900  Saabs of Anarchy                 323  49990580
61   252  WOT LEMONS                       321  49990581        1-14
62   92   Futility Motorsport              314  49990582        3-24
63   402  PunisherGP                       313  49990583        1-25
64   351  The  Cosmonaughts                309  49990584        1-24
65   86   2-Broke 2-Care                   302  49990585        3510453
66   85   Rusty Tear Racing                298  49990586        1-29
67   609  Cheesebolt Enterprises           289  49990587        4-2
68   13   Autodestruct Sequence 2          285  49990588        3-4
69   133  Cannibal Cafe Racing             279  49990589        9046109
70   147  Floodstang                       274  49990590        8746044
71   54   Somtingwong                      270  49990591        4193016
72   715  Cup Noodle Racing                260  49990592        4-3
73   928  Scuderia Craptastic              254  49990593        4-8
74   181  Garage Heroes (in training)      248  49990594        9560021
75   115  Twisted Metal                    239  49990595        1-2
76   356  Team Regressive                  222  49990596        9149634
77   100  Tuga Racing                      215  49990597        3-28
78   39   Overengineer'd Racing            194  49990598        4443990
79   444  BostonWhiners                    193  49990599        3934044
80   96   wrecktum                         178  49990600        343902
81   196  Pro Bandaid Solutions            177  49990601        702279
82   95   Scooby Doobies                   176  49990602        3-25
83   424  Old Guys with Angry Wives        168  49990603        1-26
84   299  SillyNannies                     165  49990604        1-17
85   808  Fools with tools                 161  49990605        4-5
86   2    two feathas racing               135  49990606        3-2
87   528  Lemons Plumbing and HVAC         107  49990607        4-1
88   45   Lemon Law Racing                 87   49990608        3-11
89   22   GI Driver                        80   49990609        3-6
90   104  Days Asunder (Burgess Brothers Racing) 65   49990610        1-1
91   988  FMC                              58   49990611        4-12
92   131  ITALIAN A F (As Franco)          56   49990612        1-5
93   68   Ghetto Art Rabbit                40   49990613        3-15
94   173  Bazinga Racing                   35   49990614        9237171
95   944  Got Wood                         33   49990615        4-9
96   218  Rusty Numbers formerly Bob Ross  28   49990616        1-11
97   70   Ranger Train 2                   23   49990617        3-17
98   44   Scooby Doobies                        49990618        3-9
99   78   Eastbound and Down Motorsports        49990619
100  102  The hazzards                          49990620        3-29
                                                49990621
--------------------------------------------------------------------------------
Team: WOT LEMONS Car Number: 252  Transponder: 1-14
Best Position:	84
Final Position:	61
Total Laps:	321
Best Lap:	236
Best Lap Time:	00:01:37.692
Total Time:	14:02:36.820
--------------------------------------------------------------------------------
2019-08-12 23:47:01,993 - INFO - Saving lap times to WOT LEMONS-87529-6771052.csv
--------------------------------------------------------------------------------
 Lap       LapTime Position  FlagStatus     TotalTime
   1  00:02:15.654       83           1  00:13:29.650
   2  00:02:15.958       83           1  00:15:45.608
   3  00:02:48.761       83           1  00:18:34.369
   4  02:19:15.392       90           0  02:37:49.761
   5  00:01:48.629       90           0  02:39:38.390
   6  00:01:51.982       90           0  02:41:30.372
   7  00:01:46.659       90           0  02:43:17.031
   8  00:02:01.012       90           0  02:45:18.043
   9  00:06:33.790       90           0  02:51:51.794
  10  00:01:44.440       90           0  02:53:36.234
  11  00:01:41.839       90           0  02:55:18.073
  12  00:01:46.886       90           0  02:57:04.959
  13  00:01:43.772       89           0  02:58:48.731
  14  00:01:42.112       89           0  03:00:30.843
  15  00:01:42.986       89           0  03:02:13.829
  16  00:01:40.671       89           0  03:03:54.500
  17  00:01:54.783       89           0  03:05:49.283
  18  00:01:58.326       89           0  03:07:47.609
  19  00:01:45.554       89           0  03:09:33.163
  20  00:01:46.461       88           0  03:11:19.624
  21  00:01:44.635       88           0  03:13:04.259
  22  00:01:41.628       88           0  03:14:45.887
  23  00:01:41.106       88           0  03:16:26.993
  24  00:01:40.435       86           0  03:18:07.428
  25  00:01:40.743       86           0  03:19:48.171
  26  00:01:41.149       86           0  03:21:29.320
  27  00:01:41.240       86           0  03:23:10.560
  28  00:01:41.342       84           0  03:24:51.902
  29  00:01:42.463       84           0  03:26:34.365
  30  00:01:45.567       84           0  03:28:19.932
  31  00:01:54.921       84           0  03:30:14.853
  32  00:01:47.354       83           0  03:32:02.207
  33  00:01:39.667       83           0  03:33:41.874
  34  00:01:41.257       83           0  03:35:23.131
  35  00:01:42.603       83           0  03:37:05.734
  36  00:01:40.427       82           0  03:38:46.161
  37  00:01:40.541       82           0  03:40:26.702
  38  00:01:40.934       82           0  03:42:07.636
  39  00:01:41.808       82           0  03:43:49.444
  40  00:01:42.975       82           0  03:45:32.419
  41  00:01:39.717       82           0  03:47:12.136
  42  00:01:41.585       82           0  03:48:53.721
  43  00:01:45.250       82           0  03:50:38.971
  45  00:01:50.981       82           0  03:54:11.890
  46  01:00:46.693       83           0  04:54:58.583
  47  00:08:20.322       82           0  05:03:18.837
  48  00:02:03.049       82           0  05:05:21.886
  49  00:02:01.488       82           0  05:07:23.374
  50  00:02:01.155       82           0  05:09:24.529
  51  00:02:00.588       82           0  05:11:25.117
  52  00:01:58.989       82           0  05:13:24.106
  53  00:02:05.050       82           0  05:15:29.156
  54  00:02:15.105       82           0  05:17:44.261
  55  00:01:58.794       82           0  05:19:43.055
  56  00:01:59.424       82           0  05:21:42.479
  57  00:02:02.069       82           0  05:23:44.548
  58  00:01:56.912       82           0  05:25:41.460
  59  00:01:55.339       82           0  05:27:36.799
  60  00:01:55.719       82           0  05:29:32.518
  61  00:01:56.290       82           0  05:31:28.808
  62  00:01:54.914       81           0  05:33:23.722
  63  00:01:53.116       81           0  05:35:16.838
  64  00:01:56.556       81           0  05:37:13.394
  65  00:01:53.044       81           0  05:39:06.438
  66  00:01:53.443       80           0  05:40:59.881
  67  00:01:57.044       79           0  05:42:56.925
  68  00:01:51.007       79           0  05:44:47.932
  69  00:01:52.292       79           0  05:46:40.224
  70  00:01:49.857       79           0  05:48:30.081
  71  00:01:53.368       79           0  05:50:23.449
  72  00:01:56.806       79           0  05:52:20.255
  73  00:01:53.471       79           0  05:54:13.726
  74  00:01:55.084       78           0  05:56:08.810
  75  00:01:56.993       78           0  05:58:05.803
  76  00:01:57.400       78           0  06:00:03.203
  77  00:01:53.234       78           0  06:01:56.437
  78  00:01:49.592       78           0  06:03:46.029
  79  00:02:03.606       78           1  06:05:49.635
  80  00:02:50.822       77           1  06:08:40.457
  81  00:02:10.525       77           0  06:10:50.982
  82  00:08:59.866       77           0  06:19:50.786
  83  00:01:46.212       77           0  06:21:36.998
  84  00:01:46.445       77           0  06:23:23.443
  85  00:01:45.757       77           0  06:25:09.200
  86  00:01:44.187       77           0  06:26:53.387
  87  00:01:46.929       77           0  06:28:40.316
  88  00:01:44.226       77           0  06:30:24.542
  89  00:01:43.906       77           0  06:32:08.448
  90  00:01:44.368       77           0  06:33:52.816
  91  00:01:49.524       76           0  06:35:42.340
  92  00:01:46.817       75           0  06:37:29.157
  93  00:01:46.350       75           0  06:39:15.507
  94  00:01:41.661       75           0  06:40:57.168
  95  00:01:43.607       75           0  06:42:40.775
  96  00:01:40.683       75           0  06:44:21.458
  97  00:01:40.404       75           0  06:46:01.862
  98  00:01:43.658       75           0  06:47:45.520
  99  00:01:41.473       75           0  06:49:26.993
 100  00:01:44.971       75           0  06:51:11.964
 101  00:01:44.233       75           0  06:52:56.197
 102  00:01:42.973       75           0  06:54:39.170
 103  00:01:44.895       75           0  06:56:24.065
 104  00:01:42.392       75           0  06:58:06.457
 105  00:01:39.055       75           0  06:59:45.512
 106  00:01:38.662       75           0  07:01:24.174
 107  00:01:38.087       75           0  07:03:02.261
 108  00:01:38.967       75           0  07:04:41.228
 109  00:01:58.123       74           0  07:06:39.351
 110  00:01:45.087       74           0  07:08:24.438
 111  00:01:40.393       73           0  07:10:04.831
 112  00:01:43.872       73           0  07:11:48.703
 113  00:01:43.430       73           0  07:13:32.133
 114  00:01:40.564       73           0  07:15:12.697
 115  00:01:42.256       73           0  07:16:54.953
 116  00:01:44.724       73           0  07:18:39.677
 117  00:01:41.227       73           0  07:20:20.904
 118  00:01:42.580       73           0  07:22:03.484
 119  00:01:48.813       73           0  07:23:52.297
 120  00:01:44.962       73           0  07:25:37.259
 121  00:01:48.746       73           0  07:27:26.005
 122  00:01:52.887       73           0  07:29:18.892
 123  00:01:59.521       73           0  07:31:18.413
 124  00:02:20.554       73           0  07:33:38.967
 125  00:01:47.607       73           0  07:35:26.574
 126  00:01:47.831       73           0  07:37:14.405
 127  00:02:00.136       73           0  07:39:14.541
 128  00:02:01.542       73           0  07:41:16.083
 129  00:02:10.825       73           0  07:43:26.908
 130  00:02:07.611       73           0  07:45:34.519
 131  00:01:43.058       73           0  07:47:17.577
 132  00:01:42.009       73           0  07:48:59.586
 133  00:01:44.718       73           0  07:50:44.304
 134  00:01:41.654       73           0  07:52:25.958
 135  00:01:41.799       73           0  07:54:07.757
 136  00:01:38.864       72           0  07:55:46.621
 137  00:01:42.191       72           0  07:57:28.812
 138  00:01:44.647       72           0  07:59:13.459
 139  00:01:41.405       72           0  08:00:54.864
 140  00:01:42.185       72           0  08:02:37.049
 141  00:01:40.463       72           0  08:04:17.512
 142  00:01:39.280       72           0  08:05:56.792
 143  00:01:45.704       72           0  08:07:42.496
 144  00:01:46.976       72           0  08:09:29.472
 145  00:01:41.951       72           0  08:11:11.423
 146  00:01:43.298       72           0  08:12:54.721
 147  00:01:57.775       72           0  08:14:52.496
 148  00:08:30.805       72           0  08:23:23.251
 149  00:01:47.911       72           0  08:25:11.162
 150  00:01:46.129       72           0  08:26:57.291
 151  00:01:44.382       72           0  08:28:41.673
 152  00:01:47.488       72           0  08:30:29.161
 153  00:01:46.679       72           0  08:32:15.840
 154  00:01:45.199       72           0  08:34:01.039
 155  00:01:41.057       72           0  08:35:42.096
 156  00:01:44.518       71           0  08:37:26.614
 157  00:01:42.289       71           0  08:39:08.903
 158  00:01:42.814       71           0  08:40:51.717
 159  00:01:41.490       71           0  08:42:33.207
 160  00:01:46.620       71           0  08:44:19.827
 161  00:01:47.522       71           0  08:46:07.349
 162  00:01:45.087       71           0  08:47:52.436
 163  00:01:42.265       71           0  08:49:34.701
 164  00:01:43.357       71           0  08:51:18.058
 165  00:01:43.068       71           0  08:53:01.126
 166  00:01:50.918       71           0  08:54:52.044
 167  00:01:54.081       71           0  08:56:46.125
 168  00:01:59.500       71           0  08:58:45.625
 169  00:05:19.903       71           0  09:04:05.496
 170  00:01:43.809       71           0  09:05:49.305
 171  00:01:45.469       71           0  09:07:34.774
 172  00:01:42.175       71           0  09:09:16.949
 173  00:01:47.955       71           0  09:11:04.904
 174  00:01:39.245       71           0  09:12:44.149
 175  00:01:45.302       71           0  09:14:29.451
 176  00:01:44.310       71           0  09:16:13.761
 177  00:01:42.885       70          -1  09:17:56.646
 178  17:43:46.630       70           0  09:21:59.461
 179  00:01:51.693       69           0  09:23:51.154
 180  00:01:50.198       69           0  09:25:41.352
 181  00:01:53.507       69           0  09:27:34.859
 182  00:02:03.980       69           0  09:29:38.839
 183  00:02:01.917       69           0  09:31:40.756
 184  00:01:48.491       69           0  09:33:29.247
 185  00:01:48.880       69           0  09:35:18.127
 186  00:01:49.028       69           0  09:37:07.155
 187  00:01:49.046       69           0  09:38:56.201
 188  00:01:50.100       69           0  09:40:46.301
 189  00:01:54.418       69           0  09:42:40.719
 190  00:01:49.984       69           0  09:44:30.703
 191  00:01:45.661       69           0  09:46:16.364
 192  00:01:47.579       69           0  09:48:03.943
 193  00:01:49.624       69           0  09:49:53.567
 194  00:01:46.027       69           0  09:51:39.594
 195  00:01:51.093       68           0  09:53:30.687
 196  00:01:57.447       68           1  09:55:28.134
 197  00:02:29.360       68           1  09:57:57.494
 198  00:03:05.807       68           1  10:01:03.301
 199  00:06:20.860       68           0  10:07:24.096
 200  00:01:52.502       68           0  10:09:16.598
 201  00:01:51.809       68           0  10:11:08.407
 202  00:01:52.400       68           0  10:13:00.807
 203  00:01:47.455       68           0  10:14:48.262
 204  00:01:49.295       68           0  10:16:37.557
 205  00:01:51.981       68           0  10:18:29.538
 206  00:02:03.192       68           1  10:20:32.730
 207  00:13:21.664       68           1  10:23:15.745
 208  00:02:01.168       68           0  10:25:16.913
 209  00:01:55.179       68           0  10:27:12.092
 210  00:07:51.489       68           0  10:35:03.517
 211  00:01:43.376       68           0  10:36:46.893
 212  00:01:43.262       68           0  10:38:30.155
 213  00:01:43.450       68           0  10:40:13.605
 214  00:01:43.433       68           0  10:41:57.038
 215  00:01:44.578       68           0  10:43:41.616
 216  00:01:38.336       67           0  10:45:19.952
 217  00:01:41.062       67           0  10:47:01.014
 218  00:01:41.629       66           0  10:48:42.643
 219  00:01:51.010       66           0  10:50:33.653
 220  00:02:01.165       66           0  10:52:34.818
 221  00:01:49.200       66           0  10:54:24.018
 222  00:01:43.480       66           0  10:56:07.498
 223  00:01:42.168       66           0  10:57:49.666
 224  00:01:43.789       66           0  10:59:33.455
 225  00:01:43.993       66           0  11:01:17.448
 226  00:01:41.072       66           0  11:02:58.520
 227  00:01:39.263       66           0  11:04:37.783
 228  00:01:40.162       66           0  11:06:17.945
 229  00:01:41.950       66           0  11:07:59.895
 230  00:01:42.259       66           0  11:09:42.154
 231  00:01:40.656       66           0  11:11:22.810
 232  00:01:40.905       66           0  11:13:03.715
 234  00:01:38.158       66           0  11:16:21.641
 235  00:01:39.869       66           0  11:18:01.510
 236  00:01:37.692       66           0  11:19:39.202
 237  00:01:39.232       66           0  11:21:18.434
 238  00:01:41.734       66           0  11:23:00.168
 239  00:01:43.857       66           0  11:24:44.025
 240  00:02:03.814       66           1  11:26:47.839
 241  00:04:46.071       66           0  11:31:33.830
 242  00:01:39.093       66           0  11:33:12.923
 243  00:01:38.678       66           0  11:34:51.601
 244  00:01:39.366       66           0  11:36:30.967
 245  00:01:39.083       66           0  11:38:10.050
 246  00:01:40.424       66           0  11:39:50.474
 247  00:01:54.415       66           0  11:41:44.889
 248  00:06:17.671       66           0  11:48:02.560
 249  00:01:43.604       66           0  11:49:46.164
 250  00:01:43.904       66           0  11:51:30.068
 251  00:01:43.416       66           0  11:53:13.484
 252  00:01:45.692       66           0  11:54:59.176
 253  00:01:41.368       66           0  11:56:40.544
 254  00:01:43.332       66           0  11:58:23.876
 255  00:01:41.134       66           0  12:00:05.010
 256  00:01:42.789       66           0  12:01:47.799
 257  00:01:41.851       66           0  12:03:29.650
 258  00:01:43.416       66           0  12:05:13.066
 259  00:01:41.901       65           0  12:06:54.967
 260  00:01:41.399       65           0  12:08:36.366
 261  00:01:41.885       65           0  12:10:18.251
 262  00:01:40.632       64           0  12:11:58.883
 262  00:01:40.632       64           0           12:
 263  00:01:39.802       64           0  12:13:38.685
 264  00:01:43.229       64           0  12:15:21.914
 264  00:01:43.229       64           0           12:
 265  00:01:38.476       64           0  12:17:00.390
 266  00:01:41.283       64           0  12:18:41.673
 267  00:01:41.635       64           0  12:20:23.308
 268  00:01:39.618       64           0  12:22:02.926
 269  00:01:51.804       64           0  12:23:54.730
 270  00:02:14.866       64           0  12:26:09.596
 271  00:01:51.005       63           0  12:28:00.601
 272  00:01:44.268       63           0  12:29:44.869
 273  00:01:44.252       63           0  12:31:29.121
 274  00:01:41.034       63           0  12:33:10.155
 275  00:01:43.039       62           0  12:34:53.194
 276  00:01:44.828       62           0  12:36:38.022
 277  00:01:44.935       62           0  12:38:22.957
 278  00:01:42.234       62           0  12:40:05.191
 279  00:01:41.100       62           0  12:41:46.291
 280  00:01:38.483       62           0  12:43:24.774
 282  00:01:43.304       62           0  12:46:53.521
 283  00:01:42.446       62           0  12:48:35.967
 284  00:01:44.937       62           0  12:50:20.904
 285  00:01:42.089       62           0  12:52:02.993
 286  00:01:41.759       62           0  12:53:44.752
 287  00:01:44.065       62           0  12:55:28.817
 288  00:01:43.005       62           0  12:57:11.822
 289  00:01:51.278       62           0  12:59:03.100
 290  00:10:20.989       61           0  13:09:24.089
 291  00:01:41.319       61           0  13:11:05.408
 292  00:01:42.488       61           0  13:12:47.896
 293  00:01:38.601       61           0  13:14:26.497
 294  00:01:42.541       61           0  13:16:09.038
 295  00:01:52.674       61           0  13:18:01.712
 296  00:01:42.263       61           0  13:19:43.975
 296  00:01:42.263       61           0           13:
 297  00:01:40.437       61           0  13:21:24.412
 298  00:01:41.010       61           0  13:23:05.422
 299  00:01:42.272       61           0  13:24:47.694
 300  00:01:44.127       61           0  13:26:31.821
 301  00:01:43.142       61           0  13:28:14.963
 302  00:01:42.905       61           0  13:29:57.868
 303  00:01:41.165       61           0  13:31:39.033
 304  00:01:41.519       61           0  13:33:20.552
 305  00:01:40.765       61           0  13:35:01.317
 306  00:01:43.086       61           0  13:36:44.403
 308  00:01:44.162       61           0  13:40:12.477
 309  00:01:45.897       61           0  13:41:58.374
 310  00:01:41.789       61           0  13:43:40.163
 311  00:01:42.769       61           0  13:45:22.932
 312  00:01:49.401       61           0  13:47:12.333
 313  00:01:43.900       61           0  13:48:56.233
 314  00:01:42.807       61           0  13:50:39.040
 315  00:01:44.123       61           0  13:52:23.163
 316  00:01:40.764       61           0  13:54:03.927
 317  00:01:44.453       61           0  13:55:48.380
 318  00:01:44.080       61           0  13:57:32.460
 319  00:01:40.907       61           0  13:59:13.367
 320  00:01:40.822       61           0  14:00:54.189
 321  00:01:42.631       61          -1  14:02:36.820
```
