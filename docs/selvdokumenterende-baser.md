---
title: Selvdokumenterende baser
---

# Tabeller

## Singular eller plural

Om man vil ha tabellnavn i entall eller flertall er en smakssak. Navnet
vil vises i innholdsfortegnelsen (listen over tabeller), og som
relasjoner til en post.

## Gruppering

Man kan gruppere tabeller sammen ved å gi dem samme prefix. En slik
gruppering gjenspeiler måten man ofte vil gjøre det på når man designer
databaser.

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

Disse angis med postfix `_xref` eller `_link`. Disse vises aldri i
tabellisten. Ikke uvanlig å bruke `xref` for å angi kryssreferanse, jf.
[Wikipedia](<https://en.wikipedia.org/wiki/Cross-reference>).

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
seg da i innhold under den tabellen som er referert først i
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

  F.eks. får `aktoer_naeringskategori` ledetekst `næringskategori` Da fjernes
  også postfix som `_xref`, `_list`, `_liste` eller `_link`

# Kolonner

## Usynlige

Man markerer at en kolonne ikke skal vises ved å sette en underscore
foran, eks. `_connection_string`.

Men merk at dette ikke fungerer i Oracle, da identifikatorer her må
begynne med bokstav. Dette er den eneste basen av de store som ikke
støtter dette. I Oracle kan man derimot skjule kolonner ved å definere
dem som `invisible`.

## Lengde

Biblioteket "pyodbc" som brukes i Urdr, setter alle tekstfelter som ikke
har definert lengde til `size: 255`. F.eks. gjelder det `varchar` uten
definert lengde, og `jsonb` i Postgres. Derfor vises ikke felter med
lengde 255 eller over som standard i grid.

# Fremmednøkler

Fremmednøkler brukes av Urdr for å vise fram relasjoner. Ingen regler for
navngivning av fremmednøkler.

For å vise har-mange-relasjoner, må man ha en indeks for å finne
relasjonene.

# Indekser

## Grid

For å bestemme hvilke kolonner som skal vises i grid-en, brukes indeksen
`<tabellnavn>_grid_idx` dersom den finnes.

Hvis denne indeksen ikke finnes, vises de fem første kolonnene, med
unntak av tekst-kolonner med 255 tegn eller over, skjulte kolonner, og
eventuell autoinc-kolonne. Denne siste defineres liksom i SQLite med at
den er integer og primary key.

Grensen på 255 tegn skyldes for det første at MySQL begrenser antall
tegn i indekser til dette antallet, samt at pyodbc setter lengde til 255
for tekst-kolonner som ikke har angitt lengde (f.eks. `varchar` i
Postgres og `json`).

For referansetabeller vises uansett autoinc-kolonnen også.

## Sortering

Sortering av en tabell bestemmes av indeks `<tabellnavn>_sort_idx` dersom
den finnes. Hvis den ikke finnes, og hvis `<tabellnavn>_grid_idx`
finnes, brukes de første tre kolonnene av denne som sortering. Hvis
heller ikke denne finnes, sorteres kun på primærnøkkel.

Det støttes ikke fallende sortering ennå, men det er planer om å få
det til å virke også. Noen databasemotorer støtter jo å angi asc og desc
for indeks-kolonner.

## Summering

Felter som inngår i indeksen `<tabellnavn>_summation_idx` vil bli
summert i footer til grid-en.

## Identifikasjon

Man bruker en unik indeks forskjellig fra primærnøkkel til å bestemme hva
som skal vises fra en record i en annen tabell for et fremmednøkkel-felt.

Hvis man også vil at postene skal sorteres på denne indeksen, kan man
bruke `<tabellnavn>_sort_idx` og sette denne til unik.

Hvis man har flere unike indekser, så brukes den med navn `...sort_idx`
til identifikasjon. Den andre kan da være en alternativ indeks for
fremmednøkler.

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

MySQL oppretter indekser automatisk når man genererer fremmednøkkel. Men
det er også den eneste databasen som Urdr støtter som gjør dette
automatisk. Så når Urdr krever at indeks må være på plass for å vise
relasjonen, sikres også at disse indeksene opprettes. Dette er altså
helt i tråd med Urdr sin filosofi - å effektivisere spørringer samtidig
som de definerer hvordan basen vises fram.

Man kan også definere opp en indeks `<tabellnavn>_classification_idx` som gjør
det mulig å ha en kolonne som definerer hvilken type eller klasse en post har,
og så ha en utvidelsestabell som definerer spesifikk metadata for denne typen.
Jf. [Relasjoner](#Relasjoner).

## Registrere opprettet og oppdatert

For å registrere når en post ble opprettet/endret og av hvem, kan man
sette indeksen `<tabellnavn>_created_idx` og `<tabellnavn>_updated_idx`.
Første kolonne i indeksen skal være dato eller tidsstempel, og andre
kolonne skal være brukernavn til brukeren.

Kolonnen som betegner dato eller tid, skal ha defualt-verdi satt til
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
selve nøkkelordet, og måleenheten som kommer etter verdien.

Man kan lage dynamiske lenker ved å bruke `onclick`-attributt, sammen med
`this.dataset.value`. Det er nemlig lagt inn `data-value` som attributt
til elementet som viser verdi av et felt, for å kunne brukes til dette.

Det er også mulig å bytte ut `this.dataset.value` med `this.nodeValue`.

Eks:
~~~ yml
href: '/url/to/whatever'
onclick: "location.href=this.href+'?key='+this.dataset.value;return false;"
~~~

Kan også style grid, f.eks. med bakgrunnsfarge på raden basert på verdier
i en kolonne. Merk at man da må legge til en default style, ellers vil ikke
fargene oppdateres riktig ved sortering av tabellen i etterkant.

# Views

## Bruke view til å bestemme grid

Istedenfor for definere en grid vha. indeks `<tabellnavn>_grid_idx`, kan
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

Man skal altså velge alt fra opprinnelig tabell, da denne rett og slett skal
erstattes av viewet. Metadata for viewet hentes fra opprinnelig tabell.

Man gir da brukeren tilgang til viewet, men ikke til opprinnelig tabell. Dette
forutsetter at man lager en cachet versjon av databasestrukturen først.

Man kan ha view for tilgangsstyring og view for grid samtidig. Men da bør view
for grid også ha tilgangsstyring.

# Relasjoner

For at relasjoner skal vises, må det være en indeks på de kolonnene som
definerer relasjonen. Dette er alltid tilfelle i MySQL, for der må man
ha en indeks for fremmednøkler. Dette er ikke tilfelle i PostgreSQL, så
der må man opprette indeks eksplisitt for å få visning av relasjon.

I hierarkiske strukturer hvor tabell på laveste nivå har primærnøkkel
som inneholder alle tabeller på overordnet nivå (eks. et dokument har
saksnr som del av primærnøkkelen), vil da alle relasjoner til det
laveste nivået (dokument) også vises på øverste nivå (sak). Dette fordi
indeksen som brukes for å knytte relasjonen til dokumentet også
nødvendigvis vil fungere som indeks for å hente disse relasjonene fra
øverste nivå (sak), fordi saksnr inngår i primærnøkkelen. Man kan unngå
å vise relasjoner på øverste nivå ved å legge inn prefix på relasjonen
tilsvarende tabellen relasjonen hører til (dokument). Hvis man f.eks.
har `dokument_adressat` som navn på en slik relasjonstabell, vil den kun
vises under `dokument`.

Man kan også velge å vise forenklet hierarki. Når man krysser av for dette,
vises kun de nærmeste relasjonene, og relasjoner til lavere nivåer i hierarkiet
vises ikke.

Hvis man vil vise noen relasjoner kun for visse typer poster, må man ha en
klassifisering av posten. Dette gjøres ved å ha en kolonne som brukes til
klassifisering, og sette en indeks `<tabellnavn>_classification_idx` på denne
kolonnen. Når man da har en 1:1-relasjon med navn som er lik tabellnavnet pluss
suffix som samsvarer med en klassifikasjonsverdi, vises denne relasjonen kun
når denne klassifikasjonen er satt. 

Hvis man f.eks. har en tabell `dokument` og vil angi egne metadata av
dokumenter av typen "bilde", kan man ha kolonne `type` i dokumenttabellen, med
indeks `dokument_classification_idx`. Så lager man en tabell `dokument_bilde` med
primærnøkkel `id` som refererer til `dokument.id`. Når man legger inn "bilde" som
type, vises relasjonen `dokument_bilde`.
