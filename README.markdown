Aiempi hahmotelma skriptin mahdollisesta toimintalogiikasta. Mahdollistaisi
sen, että lokeista luetaan vain uudet osat, ei jo luettua uudestaan. Kuitenkin
tarpeettoman monimutkaista?

<pre>
# alustus
if ei löydy tilaa taulusta:
   luetaan access.log.1 jos on (eka päivä heitetään hukkaan)
   luetaan access.log
else:
    # sivusto.mtime == mtime(access.log.1)
    if rotatoitu:
        seek(access.log.1, seekpoint)
        tiedostolista = [access.log.1, access.log]
    else:
        seek(access.log, seekpoint)
        tiedostolista = [access.log]

    if not md5(rivi) == md5:
        # jos rotatoitu huonosti
        alustus()
    else:
        jatketaan lukemista
</pre>

<pre>
Tilataulu (mihin kelataan)  (onko rotatoitu)
 sivusto  | seekpoint   | al1 mtime  | md5(viimeinen rivi)
  yx.fi       123899      1234567890 | abcdabbacd
         = edellisen päivän
           viimeisen rivin alku
</pre>

<pre>
Hitit
  sivusto   |   päivä     | tavuja   | hittejä
yx.fi          2009-11-10   100000      2900
yx.fi          2009-11-10   100000      2900
derbian.fi     2009-11-10   100000000   2900000
derbian.fi     2009-11-10   100000000   2900000

