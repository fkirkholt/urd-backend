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
tabellisten. Ikke uvanlig å bruke xref for å angi kryssreferanse, jf.
[Wikipedia](<https://en.wikipedia.org/wiki/Cross-reference>).

Ledetekst for slike tabeller når de vises som har-mange-relasjoner,
utledes fra navnet. Hvis navn til refererende tabell forekommer i
tabellnavnet, fjernes dette (samt evt. postfix), og så utledes ledetekst
fra det man står igjen med.

Hvis man f.eks. har tabeller `gruppe`, `bruker` og kryssreferansetabell
`bruker_gruppe_xref`, og står på en bruker, vil postfix `_xref` og
"bruker" fjernes, og vi står igjen med `gruppe`. Hvis man vil vise
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

Hvis en fremmednøkkel også inneholder primærnøkkelen, har vi det som URD
forstår som en utvidelsestabell. Det er en tabell med 1:1-relasjon til
hovedabellen. Slike tabeller begynner som regel med navnet til
hovedtabellen.

Slike tabeller er særlig aktuelle når man har felter som hører sammen,
men som ikke er obligatoriske. Da kan man trekke dem ut i en egen
tabell. F.eks. kan man ha en tabell `person` og en tabell
`person_kontakt`, hvor kontaktopplysningene legges, dersom man har slike
data.

1:1-relasjoner vises ikke som liste, men når man ekspanderer relasjonen
vises aktuelle felter isteden. Men kan jo kun registrere én post i
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

Man har en hierarkisk tabell når en fremmednøkkel i tabellen refererer
til primærnøkkel i samme tabell. Hvis man legger indeks på
fremmednøkkelen og legger en unik index på en annen kolonne i tabellen,
dvs. den kolonnen man vil bruke til å navngi posten (jf.
"Identifikasjon" under "Indekser"), så vil man få opp kun øverste nivået
når man ser på tabellen, og så kunne ekspandere hvert nivå.

## Skjulte tabeller

Man angir at en tabell skal være skjult (og kun vises i admin-modus) ved
å la tabellnavnet starte på understrek (_). Dette er det samme som
brukes f.eks. i Python for å angi at en variabel eller funksjon er
privat.

Ellers skjules også oppslagstabeller, som angis ved postfix
`_list/_liste`. Disse vises når man aktiverer admin-modus.
Kryssreferansetabeller, som angis ved postfix `_xref` eller `_link`
skjules alltid i tabllisten.

Metadatatabeller (jf. under) starter på `meta_`, og disse skjules når
man ikke er i admin-modus.

## Ledetekst til har-mange-relasjoner

- 1:M-relasjoner får tabellnavn (minus evt. prefix) pluss evt.
  kolonnenavn Trekker fra prefix med tabell som relasjonen peker til (så
  hvis man f.eks. har `arkiv` og `arkiv_serie` trekkes `arkiv_` fra og
  vi står igjen med `serie`). Dersom navnet på siste kolonne i
  fremmednøkkelen er forskjellig fra tabellnavnet som fremmednøkkelen
  refererer til, tas dette kolonnenavnet med. Eks. `registrert_av`
  istedenfor `bruker`, hvis det refereres til en `bruker`-tabell.
- M:M-relasjoner får tabellnavnet, minus evt. prefix/postfix for aktiv
  tabell F.eks. får `aktoer_naeringskategori` ledetekst
  `næringskategori` Da fjernes også postfix som `_xref`, `_list`,
  `_liste` eller `_link`

# Kolonner

## Usynlige

Man markerer at en kolonne ikke skal vises ved å sette en underscore
foran, eks. `_connection_string`. Dette harmonerer godt med hvordan man
markerer private variabler i en Python-klasse.

Men merk at dette ikke fungerer i Oracle, da identifikatorer her må
begynne med bokstav. Dette er den eneste basen av de store som ikke
støtter dette. I Oracle kan man derimot skjule kolonner ved å definere
dem som `invisible`.

## Lengde

Biblioteket "pyodbc" som brukes i URD, setter alle tekstfelter som ikke
har definert lengde til `size: 255`. F.eks. gjelder det `varchar` uten
definert lengde, og `jsonb` i Postgres. Derfor vises ikke felter med
lengde 255 eller over som standard i grid.

# Fremmednøkler

Fremmednøkler brukes av URD for å vise fram relasjoner. Ingen regler for
navngivning av relasjoner.

For å vise har-mange-relasjoner, må man ha en index for å finne
relasjonene.

# Indekser

## Grid

For å bestemme hvilke kolonner som skal vises i grid-en, brukes indexen
`<tabellnavn>_grid_idx` dersom den finnes.

Hvis denne indeksen ikke finnes, vises de fem første kolonnene, med
unntak av tekst-kolonner med 255 tegn eller over, skjulte kolonner, og
eventuell autoinc- kolonne. Denne siste defineres liksom i SQLite med at
den er integer og primary key.

Grensen på 255 tegn skyldes for det første at MySQL begrenser antall
tegn i indekser til dette antallet, samt at pyodbc setter lengde til 255
for tekst-kolonner som ikke har angitt lengde (f.eks. `varchar` i
Postgres og `json`).

For referansetabeller vises uansett autoinc-kolonnen også.

## Sortering

Sortering av en tabell bestemmes av index `<tabellnavn>_sort_idx` dersom
den finnes. Hvis den ikke finnes, og hvis `<tabellnavn>_grid_idx`
finnes, brukes de første tre kolonnene av denne som sortering. Hvis
heller ikke denne finnes, sorteres kun på primærnøkkel.

Det støttes ikke descending sortering ennå, men det er planer om å få
det til å virke også. Noen databasemotorer støtter jo å angi asc og desc
for index-kolonner.

## Summering

Felter som inngår i indeksen `<tabellnavn>_summation_idx` vil bli
summert i footer til grid-en.

@note Ikke veldig nødvendig å ha en indeks på disse for hastighetens
skyld. Summering kan gå litt raskere når man har index, slik at alle
radene i indeksen kan gjennomgås istedenfor alle radene i tabellen. Men
vi har som regel indeks som angir hvilke kolonner vi vil se i tabellen
uansett.

## Identifikasjon

Man bruker en unik index forskjellig fra primærnøkkel til å bestemme hva
som skal vises fra en record i en annen tabell for et
fremmednøkkel-felt.

Hvis man også vil at postene skal sorteres på denne indeksen, kan man
bruke `<tabellnavn>_sort_idx` og sette denne til unik.

Hvis man har flere unike indekser, så brukes den med navn `...sort_idx`
til identifikasjon. Den andre kan da være en alternativ index for
fremmednøkler.

## Lenke til fil

For å identifisere et felt som en filbane, kan man legge inn index
`<tabellnavn>_filepath_idx`.

Dette tillater også at man setter sammen filbanen fra flere kolonner,
f.eks. en kolonne som betegner sti til mappen hvor filen befinner seg,
og en som betegner filnavn. Da opprettes indexen på alle disse
kolonnene. Man må angi kolonnene i den rekkefølgen som brukes i
filbanen.

Hvis man bruker SQLite, kan man angi stien relativt til stien til
SQLite-filen.

## Vise har-mange-relasjoner

Fremmednøkler bør være knytta til indekser når man man skal gå andre
veien i en fk-relasjon, dvs. vise har-mange-relasjoner. Indeksene brukes
altså til å hente opp alle relasjoner. URD viser ikke fram slike
relasjoner med mindre det finnes en index som kan brukes for å finne
dem. Hvis det ikke eksisterer en indeks på samme kolonner som
fremmednøkkelen, vises relasjonen kun fra refererende tabell.

MySQL oppretter indekser automatisk når man genererer fremmednøkkel. Men
det er også den eneste databasen som URD støtter som gjør dette
automatisk. Så når URD krever at indeks må være på plass for å vise
relasjonen, sikres også at disse indeksene opprettes. Dette er altså
helt i tråd med URD sin filosofi - å effektivisere spørringer samtidig
som de definerer hvordan basen vises fram.

## Registrere opprettet og oppdatert

For å registrere når en post ble opprettet/endret og av hvem, kan man
sette indeksen `<tabellnavn>_created_idx` og `<tabellnavn>_updated_idx`.
Første kolonne i indeksen skal være dato eller tidsstempel, og andre
kolonne skal være brukernanv til brukeren.

Kolonnen som betegner dato eller tid, skal ha defualt-verdi satt til
`current_date` eller `current_timestamp`. Kolonnen som betegner
brukernavn får brukernanv til pålogget bruker.

# Terminologi

Har en egen tabell `meta_term` for å håntere beskrivelse, særlig
ettersom det ikke finnes noen felles sql-standard for å legge til
beskrivelser til tabeller og kolonner i databasen.

Dessuten gir det mulighet til å beskrive mer (f.eks. prefixer).

Tabellen består kun av tre kolonner:

- term (pk)
- label
- attributes

Tabellen beskriver altså terminologien som er brukt i databasen. Det kan
være et fagsystem med egen terminologi. Man kan angi tabell- og
kolonnenavn, samt tabell- og kolonne-prefixer.

Vi spesifiserer altså ikke nødvendigvis hvilket objekt dette gjelder.
Det er kun terminologien som beskrives. Dvs. at når man har to kolonner
med samme navn i forskjellige tabeller, trenger label for dette bare å
beskrives én gang.

Men hvis man trenger å beskrive to kolonner med samme navn forskjellig,
kan man legge til tabellnavnet først i "term"-kolonnen, og slik angi
`tabellnavn.kolonnenavn`, dvs. på samme måte som man angir en kolonne i
en sql-setning.

I kolonnen "attributes" kan man angi html-attributter i YAML. Her kan
man f.eks. angi en beskrivelse av et felt med "title"-attributtet, så
blir beskrivelsen til feltet vist når man holder muspekeren over
ledeteksten eller feltet. Man kan også angi Tachyons-klasser for å tune
utseendet, jf <https://tachyons.io/docs/>

Bare noen få attributter støttes foreløpig:

- class

  Brukes på input-felter i postskjemaet og celler i tabellen

- style

  Brukes på input-felter i postskjemaet

- title

  Brukes til feltbeskrivelse i postvisning/postskjema

- pattern

  Brukes på input-felter av typen 'text' i postskjemaet

Man kan også legge inn attributtet "data-format" med verdi "markdown"
for å angi at et felt skal formateres med Markdown.

SQL for å opprette terminologi-tabellen:

``` sql
create table meta_term
(
    term varchar(100) not null,
    label varchar(100),
    attributes varchar(1000),
    primary key (term)
);
```

# Metadata

Har en tabell `meta_data` med følgende kolonner:

- `_name`
- `label`
- `description`
- `cache`

`_name` angir databasenavnet. Sørger for at man ikke kan ha flere rader
som beskriver databasen. Understrek først i navnet medførerer at
kolonnen ikke vises i URD.

`label` angir hvordan navnet på databasen skal vises i URD.

`description` brukes til å beskrive databasen. Denne vises når man går
inn på en database.

`cache` holder en json-versjon av databasestrukturen. Genereres via
dialog for å oppdatere skjema fra base.

Tabellen genereres automatik dersom man genererer et cache av
databasestrukturen. Ellers kan den også opprettes manuelt:

``` sql
DROP TABLE IF EXISTS meta_data;
CREATE TABLE meta_data (
    _name varchar(30) NOT NULL,
    label varchar(30) NOT NULL,
    description text NOT NULL,
    cache json,
    PRIMARY KEY (_name)
);
```

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

# Relasjoner

For at relasjoner skal vises, må det være en index på de kolonnene som
definerer relasjonen. Dette er alltid tilfelle i MySQL, for der må man
ha en index for fremmednøkler. Dette er ikke tilfelle i PostgreSQL, så
der må man opprette index eksplisitt for å få visning av relasjon.

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

Hvis man vil vise noen relasjoner kun for visse typer poster, kan man ha
en kolonne som viser type både i hovedtabellen og i relasjonstabellene.
I sistnevnte opprettes en kolonne med konstant verdi lik typen man vil
vise relasjon for. Dette gjøres ved å sette default verdi på kolonnen
til typen, og så angi kolonnenavnet med prefix "const\_" eller "\_".
(Sistnevnte angir usynlig kolonne, og kan brukes for de databasene som
støtter det. Kolonner med navn som starter på "const\_" vil heller ikke
vises i brukergrensesnittet.) Så lar man type-feltet være en del av
fremmednøkkelen til hovedtabellen.

Hvis man f.eks. har en tabell `dokument` og vil angi egne metadata av
dokumenter av typen "bilde", kan man ha kolonne `type` i
dokumenttabellen. Så lager man en tabell `dokument_bilde` med kolonne
`id` som refererer til `dokument.id`, samt en kolonne `_type` eller
`const_type` med standardverdi "bilde" som refererer til
`dokument.type`.
