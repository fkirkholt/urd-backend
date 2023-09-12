---
title: Selvdokumenterende baser
---

# Tabeller

## Singular eller plural

Om man vil ha tabellnavn i entall eller flertall er en smakssak. Navnet
vil vises i innholdsfortegnelsen (listen over tabeller), og som
relasjoner til en post.

## Gruppering

Man kan gruppere tabeller sammen ved å gi dem samme prefiks. En slik
gruppering gjenspeiler måten man ofte vil gjøre det på når man designer
databaser. Tabeller med samme prefix vil legge seg i innholdsfortegnelsen
til venstre med prefikset som overskrift.

Ellers kan man definere underordnede tabeller ved at fremmednøkler
inngår i primærnøkkelen. Slike underordnede tabeller vil legge seg under
hovedtabellen slik at de vises når denne ekspanderes.

## Oppslagstabeller

Oppslagstabeller angis med postfix `_list/_liste`. Disse vises kun når
man aktiverer admin-modus. Da vises de med liste-ikon.

Når man bruker postfix til å angi oppslagstabeller, ser man også med én
gang hva slags type tabell dette er, når man ser på dem i en
databaseklient.

En annen måte å angi at noe er en referansetabell, er å sette datatypen
til primærnøkkel-kolonnen til en tekst-type, f.eks. `char`, `varchar`
eller `text`.

Når man setter primærnøkkel til tekst, kan man også gi en gjenkjennbar
kode som kan brukes i komprimert tabellvisning i Urdr. Da ser man kun
koden og ikke betegnelsen til posten.

## Kryssreferanse-tabeller

Kryssreferanse-tabeller angis med postfix `_xref` eller `_link`. De vises
aldri i tabellisten.

Ledetekst for slike tabeller når de vises som har-mange-relasjoner,
utledes fra navnet. Hvis navn til refererende tabell forekommer i
tabellnavnet, fjernes dette (samt evt. postfix), og så utledes ledetekst
fra det man står igjen med.

Hvis man f.eks. har tabeller `gruppe`, `bruker` og kryssreferansetabell
`bruker_gruppe_xref`, og står på en bruker, vil postfix `_xref` og
`bruker` fjernes, og vi står igjen med "gruppe". Hvis man vil vise
"brukergruppe" isteden, kaller man tabellen det.

Hvis man har en kryssreferansetabell som man likevel vil vise i innhold,
kan man gi den det navn man ønsker, og sløyfe postfix. Tabellen legger
seg da i innholdslisten under den tabellen som er referert først i
primærnøkkelen.

F.eks. hvis man har en tabell "Arkivskaper" med primærnøkkel
`aktoer, arkiv`, legger tabellen "Arkivskaper" seg under tabellen
"Aktør". Hvis man vil den skal legge seg under "Arkiv" isteden, settes
primærnøkkel til `arkiv, aktoer`.

Man bør også gruppere kolonnene slik at kolonnene i primærnøkkelen står
i samme rekkefølge i databasen som de gjør i primærnøkkelen.

## Utvidelsestabeller

Hvis en fremmednøkkel også inneholder primærnøkkelen, har vi det som Urdr
forstår som en utvidelsestabell. Det er en tabell med 1:1-relasjon til
hovedabellen. Slike tabeller begynner som regel med navnet til
hovedtabellen.

Slike tabeller er særlig aktuelle når man har felter som hører sammen,
men som ikke er obligatoriske. Da kan man trekke dem ut i en egen
tabell. F.eks. kan man ha en tabell `person` og en tabell
`person_kontakt`, hvor kontaktopplysningene legges, dersom man har slike
data.

1:1-relasjoner vises ikke som liste. Når man ekspanderer relasjonen
vises aktuelle felter isteden. Man kan jo kun registrere én post i
1:1-relasjoner.

Utvidelsestabeller vises ikke på øverste nivå i innholdslisten, men
under sine overordnede tabeller, liksom andre underliggende tabeller.
Hvis man vil at en utvidelsestabell ikke skal vises i innholdslisten,
kan man gi den postfix `_ext`.

## Underordnede tabeller

Tabeller hvor hele eller deler av primærnøkkelen også representerer
fremmednøkkel til andre tabeller, reknes som underordnede tabeller.
Disse vises i innholdsfortegnelsen under den øverste tabellen i
hierarkiet.

Kryss-referanse-tabeller behandles dermed også som underordnede
tabeller.

## Hierarkiske tabeller

Man har en hierarkisk tabell når en fremmednøkkel i tabellen refererer til
primærnøkkel i samme tabell. Hvis man legger indeks på fremmednøkkelen og
legger en unik indeks på en annen kolonne i tabellen, dvs. den kolonnen man vil
bruke til å navngi posten (jf. [Identifikasjon](#Identifikasjon)), så vil man
få opp kun øverste nivået når man ser på tabellen, og så kunne ekspandere hvert
nivå.

## Skjulte tabeller

Man angir at en tabell skal være skjult (og kun vises i admin-modus) ved å la
tabellnavnet starte på understrek (_). Dette harmonerer med en praksis i
enkelte programmeringsspråk med at variabler som starter med understrek skal
betraktes som private variabler.

Ellers skjules også oppslagstabeller, som angis ved postfix `_list/_liste`.
Disse vises når man aktiverer admin-modus. Kryssreferansetabeller, som angis
ved postfix `_xref` eller `_link`, skjules alltid i tabllisten.

## Ledetekst til har-mange-relasjoner

- 1:M-relasjoner får tabellnavn (minus evt. prefix) pluss evt. kolonnenavn

  Trekker fra prefix med tabell som relasjonen peker til (så hvis man f.eks.
  har `arkiv` og `arkiv_serie` trekkes `arkiv_` fra og vi står igjen med
  `serie`). Dersom navnet på siste kolonne i fremmednøkkelen er forskjellig fra
  tabellnavnet som fremmednøkkelen refererer til, tas dette kolonnenavnet med.
  Eks. `registrert_av` istedenfor `bruker`, hvis det refereres til en
  `bruker`-tabell.

- M:M-relasjoner får tabellnavnet, minus evt. prefix/postfix for aktiv tabell 

  F.eks. får `aktoer_naeringskategori` ledetekst `næringskategori`. Da fjernes
  også postfix som `_xref`, `_list`, `_liste` eller `_link`

# Kolonner

## Usynlige

Man markerer at en kolonne ikke skal vises ved å sette en underscore
foran, eks. `_connection_string`.

Men merk at dette ikke fungerer i Oracle, da identifikatorer her må begynne
med bokstav. I Oracle kan man derimot skjule kolonner ved å definere dem som
`invisible`.

# Fremmednøkler

Fremmednøkler brukes av Urdr for å vise fram relasjoner.

For å vise har-mange-relasjoner, må man ha en indeks for å finne
relasjonene.

# Indekser

Urdr bruker i stor grad indekser for å vite hvordan data skal vises fram.

## Grid

For å bestemme hvilke kolonner som skal vises i grid-en, brukes indeksen
`<tabellnavn>_grid_idx` dersom den finnes.

Hvis denne indeksen ikke finnes, vises de fem første kolonnene, med
unntak av tekst-kolonner med 255 tegn eller over, skjulte kolonner, og
eventuell autoinc-kolonne. Denne siste defineres liksom i SQLite med at
den er integer og primary key.

Grensen på 255 tegn skyldes at MySQL begrenser antall
tegn i indekser til dette antallet.

For referansetabeller vises uansett autoinc-kolonnen også.

## Sortering

Sortering av en tabell bestemmes av indeks `<tabellnavn>_sort_idx` dersom
den finnes. Hvis den ikke finnes, og hvis `<tabellnavn>_grid_idx`
finnes, brukes de første tre kolonnene av denne som sortering. Hvis
heller ikke denne finnes, sorteres kun på primærnøkkel.

Man kan angi sorteringsretning i indeksene for de databasene som støtter
dette.

## Summering

Felter som inngår i indeksen `<tabellnavn>_summation_idx` vil bli
summert i footer til grid-en.

## Identifikasjon

Man bruker en unik indeks forskjellig fra primærnøkkel til å bestemme hva
som skal vises fra posten i et fremmednøkkelfelt i en refererende tabell.

Hvis man også vil at postene skal sorteres på denne indeksen, kan man
bruke `<tabellnavn>_sort_idx` og sette denne til unik.

Hvis man har flere unike indekser, så brukes den med navn `...sort_idx`
til identifikasjon.

## Lenke til fil

For å identifisere et felt som en filbane, kan man legge inn indeks
`<tabellnavn>_filepath_idx`.

Dette tillater også at man setter sammen filbanen fra flere kolonner,
f.eks. en kolonne som betegner sti til mappen hvor filen befinner seg,
og en som betegner filnavn. Da opprettes indeksen på alle disse
kolonnene. Man må angi kolonnene i den rekkefølgen som brukes i
filbanen.

Hvis man bruker SQLite, kan man angi stien relativt til stien til
SQLite-filen.

Hvis man vil generere filnavn fra en sti og en kolonne i tabellen,
kan man opprette en generert kolonne.

## Vise har-mange-relasjoner

Fremmednøkler bør være knytta til indekser når man man skal gå andre
veien i en fk-relasjon, dvs. vise har-mange-relasjoner. Indeksene brukes
altså til å hente opp alle relasjoner. Urdr viser ikke fram slike
relasjoner med mindre det finnes en indeks som kan brukes for å finne
dem. Hvis det ikke eksisterer en indeks på samme kolonner som
fremmednøkkelen, vises relasjonen kun fra refererende tabell.

MySQL og MariaDB oppretter indekser automatisk når man genererer
fremmednøkkel. Men det er også de eneste databasene som Urdr støtter som
gjør dette automatisk. Så når Urdr krever at indeks må være på plass
for å vise relasjonen, sikres også at disse indeksene opprettes. Dette
er altså helt i tråd med Urdr sin filosofi - å effektivisere spørringer
samtidig som de definerer hvordan basen vises fram.

## Registrere opprettet og oppdatert

For å registrere når en post ble opprettet/endret og av hvem, kan man
sette indeksen `<tabellnavn>_created_idx` og `<tabellnavn>_updated_idx`.
Første kolonne i indeksen skal være dato eller tidsstempel, og andre
kolonne skal være brukernavn til brukeren.

Kolonnen som betegner dato eller tid, skal ha default-verdi satt til
`current_date` eller `current_timestamp`. Kolonnen som betegner
brukernavn skal ha default-verdi `current_user`.

# HTML-attributter

Man kan definere html-attributter i tabellen `html_attributes`. Denne
kan enten opprettes manuelt, eller man kan opprette en cachet versjon av
databasestrukten, som da vil generere denne tabellene. Cachen legges i denne
tabellen under selector `base`.

Tabellen har kun to kolonner: `selector` og `attributes`. Førstnevnte er
css-selector. Her kan angis css selector for DOM-elementer. De ulike feltene
og feltsettene har fått navn slik at det skal være enkelt å velge dem
med en css-selector.

I kolonnen `attributes` kan man angi alle mulige html-attributter for
valgte elementer. Disse vil så tilordnes elementene når siden tegnes
opp. Attributtene legges inn som yaml.

Ettersom Urdr støtter [Tachyons](https://tachyons.io/), kan man
angi Tachyons-klasser her. De fleste elementene er fra før stylet med
Tachyons-klasser, så de klassene som angis her, vil erstatte dem som er i
koden. Man kan inspisere et element på siden for å se hvilke klasser som
er brukt fra før, så kan man evt. kopiere disse, og erstatte dem man vil.

Hvert felt i postvisningen er omslutta av en `label`-tagg; dette betegnes som
indirekte label. Det er gjort slik for å kunne koble label til input. Vi
kan ikke bruke `for`-attributtet for å knytte label til riktig input, for
dette krever unik id, og med den fleksibiliteten som er i Urdr kan man lett
få samme id to ganger.

For å kunne style selve ledeteksten, er denne lagt inn i en `b`-tagg.
Denne brukes i moderne html bl.a. for å framheve nøkkelord. Og den uthevede
delen av en ledetekst er å rekne som nøkkelord.

Det er lagt inn mulighet for å legge til tekst før eller etter nøkkelordet
til en label. Dette gjøres ved å legge til attributt `data-before` eller
`data-after` med ønsket tekst i `b`-elementet under `label`. Dette gjør
det mulig å legge til kolon etter label, eller stjerne for å markere at
et felt er obligatorisk. Sistnevnte kan oppnås med selector `label b:has(+
[required])` sammen med attributt `data-after: '＊'`.

Man kan også legge til f.eks. en måleenhet etter et felt, ved å legge inn
måleenheten i `data-after`-attributt til label. Label består da både av
selve nøkkelordet, og måleenheten som kommer etter feltverdien.

Man kan tilpasse hvordan et felt vises ved å angi `data-type` og/eller
`data-format`. Dette forutsetter at  man bruker selector på formen
`label[data-field="tabellnavn.feltnavn"]`. Denne selectoren tilhører label-
taggen, som omslutter feltet. Man kan evt. sløyfe `label` og kun bruke
`[data-field="tabellnavn.feltnavn"]`. Da velges html-element for feltet
basert på verdiene til `data-type` og `data-format`.

Det støttes følgende verdier for `data-type`:
- json
- date

Det støttes følgende verdier for `data-format`:
- link
- json
- yaml
- markdown

Hvis man angir `data-type` som "json" og `data-format` som "yaml", vil
data lagres som `json` i databasen, men man vil se dataene som `yaml`.
Dette gjelder som standard html-attributtene selv.

Man kan angi `data-type: date` dersom man har en tekst-kolonne i databasen som
bruker til å angi dato. En html `<time>`-tagg tillater nemlig å registrere
datoer på flere måter enn mange databaser, f.eks. "2012-05" som står for
mai 2012. Disse kan da registreres som tekst i basen.

Hvis man ønsker å lage en url av et felt, kan man registrere det med
`data-format: link`. Da får man en `<a>`-tagg rundt feltverdien i
visningsmodus.

Så kan man lage dynamiske lenker til denne `<a>`-taggen ved å bruke
`onclick`-attributt, sammen med `this.dataset.value`. Det er nemlig lagt inn
`data-value` som attributt til elementet som viser verdi av et felt, for å
kunne brukes til dette. Det er også mulig å bytte ut `this.dataset.value`
med `this.innerHTML`.

Selector blir da på formen `label[data-field="tabellnavn.feltnavn"] > a`,
og attributtene kan være noe som dette:

~~~ yml
href: '/url/to/whatever'
onclick: "location.href=this.href+'?key='+this.dataset.value;return false;"
~~~

Man kan også style grid, f.eks. med bakgrunnsfarge på raden basert på
verdier i en kolonne. Merk at man da må legge til en default style, ellers
vil ikke fargene oppdateres riktig ved sortering av tabellen i etterkant.

# Views

## Bruke view til å bestemme grid

Istedenfor for definere en grid vha. indeks `<tabellnavn>n_grid_idx`, kan
man bruke et view `<tabellnavn>_grid`. Dette viewet må ha med alle
primærnøkkel-kolonnene til opprinnelig tabell. Fordelen med å bruke et
view istedenfor en indeks, er at man kan definere opp kolonner som ikke
finnes i opprinnelig tabell. Slik kan man få inn f.eks. statistikk,
antall underliggende, mm.

Alle ekstra kolonner i viewet blir også tilgjengelig i postvisningen, og
blir søkbare.

## Bruke view til tilgangsstyring

Hvis man oppretter et view med navn `<tabellnavn>_view`, vil dette viewet
erstatte tabellen når man foretar spørringer. Man kan dermed legge inn
tilgangsstyring i dette viewet.

Eksempel:

~~~ sql
create view serie_view as
select * from serie
where serie.skjerming is null or
serie.skjerming in (
select skjerming from bruker_skjerming
where brukernavn = current_user()
);
~~~

Man skal altså velge alt fra opprinnelig tabell, da denne rett og slett
skal erstattes av viewet. Metadata for viewet hentes fra opprinnelig tabell.

Man gir da brukeren tilgang til viewet, men ikke til opprinnelig tabell. Dette
forutsetter at man lager en cachet versjon av databasestrukturen først.

Man kan ha view for tilgangsstyring og view for grid samtidig. Men da bør
view for grid også ha tilgangsstyring.

