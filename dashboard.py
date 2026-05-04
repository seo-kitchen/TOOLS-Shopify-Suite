"""
Serax Product Onboarding Dashboard — SOP v2.7
Interactief CLI dashboard dat het volledige pipeline-proces begeleidt.

Gebruik:
    python dashboard.py
    python dashboard.py --fase 3          (sla fase-vraag over)
    python dashboard.py --fase 3 --vanaf match   (start vanaf een bepaalde stap)
"""

import argparse
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.rule import Rule
from rich.text import Text
from rich.prompt import Prompt, Confirm
from rich import box
from rich.columns import Columns
from rich.align import Align

load_dotenv()

# Forceer UTF-8 output op Windows
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr.encoding != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

console = Console(highlight=False)

# ── Helpers ───────────────────────────────────────────────────────────────────

def supabase():
    from supabase import create_client
    return create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))


def header():
    console.clear()
    console.print()
    console.print(Panel.fit(
        "[bold white]SERAX PRODUCT ONBOARDING PIPELINE[/bold white]\n"
        "[dim]SOP v2.7 — Interactief Dashboard[/dim]",
        border_style="blue",
        padding=(1, 4),
    ))
    console.print()


def stap_banner(num: int, naam: str, kleur: str = "blue"):
    console.print()
    console.rule(f"[bold {kleur}]STAP {num} — {naam.upper()}[/bold {kleur}]", style=kleur)
    console.print()


def wat_gaan_we_doen(tekst: str):
    console.print(Panel(
        tekst,
        title="[bold cyan]Wat gaan we doen?[/bold cyan]",
        border_style="cyan",
        padding=(0, 2),
    ))
    console.print()


def resultaat_panel(tekst: str, titel: str = "Resultaat", ok: bool = True):
    kleur = "green" if ok else "yellow"
    console.print(Panel(
        tekst,
        title=f"[bold {kleur}]{titel}[/bold {kleur}]",
        border_style=kleur,
        padding=(0, 2),
    ))
    console.print()


def review_moment(vraag: str = "Alles goed? Wil je doorgaan naar de volgende stap?") -> bool:
    console.print(Rule("[bold yellow]REVIEW MOMENT[/bold yellow]", style="yellow"))
    console.print()
    ga_door = Confirm.ask(f"[bold yellow]{vraag}[/bold yellow]")
    console.print()
    return ga_door


def aanpassing_nodig(instructie: str):
    console.print(Panel(
        f"[yellow]{instructie}[/yellow]",
        title="[bold yellow]Aanpassing nodig[/bold yellow]",
        border_style="yellow",
        padding=(0, 2),
    ))
    Prompt.ask("[dim]Druk op Enter als je klaar bent om opnieuw te proberen[/dim]")


def stop_dashboard():
    console.print()
    console.print(Panel(
        "[bold red]Dashboard gestopt.[/bold red]\n"
        "[dim]Je kunt het dashboard opnieuw starten wanneer je klaar bent.[/dim]",
        border_style="red",
        padding=(1, 2),
    ))
    sys.exit(0)


def bestand_vragen(label: str, verplicht: bool = True) -> str | None:
    while True:
        pad = Prompt.ask(f"[cyan]{label}[/cyan]", default="" if not verplicht else None)
        if not pad:
            if not verplicht:
                return None
            console.print("[red]Dit bestand is verplicht.[/red]")
            continue
        path = Path(pad.strip().strip('"'))
        if path.exists():
            return str(path)
        console.print(f"[red]Bestand niet gevonden: {path}[/red]")
        if not verplicht:
            overslaan = Confirm.ask("[dim]Overslaan?[/dim]")
            if overslaan:
                return None


def db_status_tabel(fase: str | None = None) -> Table:
    """Laat een tabel zien met de huidige database-status."""
    sb = supabase()
    tabel = Table(box=box.ROUNDED, show_header=True, header_style="bold blue", expand=False)
    tabel.add_column("Tabel", style="cyan")
    tabel.add_column("Rijen", justify="right")
    tabel.add_column("Status", justify="center")

    checks = [
        ("seo_category_mapping",  "Categorieën geladen?", 14),
        ("seo_shopify_index",     "Website-structuur geladen?", 1),
        ("seo_website_collections", "Collecties geladen?", 1),
        ("seo_filter_values",     "Filterwaarden geladen?", 1),
    ]

    if fase:
        checks += [
            (f"seo_products (fase {fase})", f"Producten fase {fase}", 0),
        ]

    for tabel_naam, omschrijving, minimum in checks:
        # Haal kolomnaam op (fase-check speciaal behandelen)
        if tabel_naam.startswith("seo_products ("):
            r = sb.table("seo_products").select("id").eq("fase", fase).execute()
        else:
            r = sb.table(tabel_naam).select("id").execute()

        count = len(r.data) if r.data else 0
        ok    = count >= minimum
        icon  = "[green]✓[/green]" if ok else "[red]✗[/red]"
        tabel.add_row(omschrijving, str(count), icon)

    return tabel


def fase_producten_status(fase: str) -> Table:
    """Tabel met status-verdeling van producten in deze fase."""
    sb = supabase()
    result = sb.table("seo_products").select("status,status_shopify").eq("fase", fase).execute()

    status_counts    = {}
    shopify_counts   = {}
    for r in result.data:
        s  = r.get("status") or "?"
        ss = r.get("status_shopify") or "onbekend"
        status_counts[s]  = status_counts.get(s, 0) + 1
        shopify_counts[ss] = shopify_counts.get(ss, 0) + 1

    tabel = Table(box=box.SIMPLE, header_style="bold", expand=False)
    tabel.add_column("Pipeline Status", style="cyan")
    tabel.add_column("Aantal", justify="right")
    for k, v in sorted(status_counts.items()):
        kleur = {"raw": "white", "ready": "green", "review": "yellow"}.get(k, "dim")
        tabel.add_row(f"[{kleur}]{k}[/{kleur}]", str(v))

    tabel2 = Table(box=box.SIMPLE, header_style="bold", expand=False)
    tabel2.add_column("Shopify Status", style="cyan")
    tabel2.add_column("Aantal", justify="right")
    for k, v in sorted(shopify_counts.items()):
        kleur = {"actief": "red", "archief": "yellow", "nieuw": "green", "onbekend": "dim"}.get(k, "white")
        tabel2.add_row(f"[{kleur}]{k}[/{kleur}]", str(v))

    return tabel, tabel2


# ── Stap-functies ─────────────────────────────────────────────────────────────

def stap_0_welkom_en_check():
    header()
    console.print(Panel(
        "[white]Dit dashboard begeleidt je stap voor stap door het Serax product onboarding proces.\n\n"
        "Na elke stap zie je de resultaten en beslis jij of we doorgaan.\n"
        "Bij twijfelgevallen word je expliciet gevraagd om een beslissing.\n\n"
        "[bold red]Elke fout kan impact hebben op de webshop — neem de tijd voor elke review.[/bold red][/white]",
        title="[bold blue]Welkom bij de Serax Product Onboarding Pipeline[/bold blue]",
        border_style="blue",
        padding=(1, 3),
    ))
    console.print()

    console.print("[bold]Database-status:[/bold]")
    console.print(db_status_tabel())
    console.print()

    if not Confirm.ask("[bold]Doorgaan met het dashboard?[/bold]"):
        stop_dashboard()


def stap_setup_check(fase: str) -> bool:
    """Controleert of eenmalige setup is gedaan. Geeft True als OK."""
    sb = supabase()

    cat_count  = len(sb.table("seo_category_mapping").select("id").execute().data or [])
    idx_count  = len(sb.table("seo_shopify_index").select("id").execute().data or [])

    problemen = []
    if cat_count < 14:
        problemen.append(f"Categorieën niet geladen (gevonden: {cat_count}, verwacht: 14)")
    if idx_count == 0:
        problemen.append("Shopify-index is leeg — website-structuur nog niet geladen")

    if not problemen:
        return True

    console.print(Panel(
        "\n".join(f"[red]✗ {p}[/red]" for p in problemen),
        title="[bold red]Setup nog niet compleet[/bold red]",
        border_style="red",
        padding=(0, 2),
    ))
    console.print()

    return False


def stap_1_setup(fase: str):
    stap_banner(1, "Eenmalige Setup", "blue")
    wat_gaan_we_doen(
        "We controleren of de eenmalige setup klaar is:\n\n"
        "  [cyan]1a.[/cyan] Categorieën uit SOP v2.7 in de database (14 regels)\n"
        "  [cyan]1b.[/cyan] Shopify-websitestructuur geladen (matching-index + collecties + filterwaarden)\n\n"
        "[dim]Als dit al gedaan is, slaan we dit stap over.[/dim]"
    )

    sb = supabase()

    # 1a. Categories
    cat_count = len(sb.table("seo_category_mapping").select("id").execute().data or [])
    if cat_count >= 14:
        console.print(f"  [green]✓[/green] Categorieën: {cat_count} regels aanwezig")
    else:
        console.print(f"  [yellow]![/yellow] Categorieën: slechts {cat_count} regels — seeden...")
        from execution.seed_categories import seed
        seed()
        console.print(f"  [green]✓[/green] Categorieën geladen")

    # 1b. Website-structuur
    idx_count = len(sb.table("seo_shopify_index").select("id").execute().data or [])
    if idx_count > 0:
        console.print(f"  [green]✓[/green] Shopify-index: {idx_count} producten aanwezig")
    else:
        console.print()
        console.print("[yellow]De Shopify-index is leeg. We moeten de website-structuur inladen.[/yellow]")
        console.print("[dim]Je hebt de Shopify webshop-export CSV nodig (Admin > Producten > Export > Alle producten)[/dim]")
        console.print()

        webshop_csv = bestand_vragen("Pad naar actieve webshop export (CSV)", verplicht=True)
        archief_csv = bestand_vragen("Pad naar archief export (CSV, optioneel)", verplicht=False)

        console.print()
        console.print("[cyan]Website-structuur laden...[/cyan]")
        from execution.load_website_structure import load_website_structure
        load_website_structure(webshop_csv, archief_csv)

    console.print()
    console.print("[bold]Database-status na setup:[/bold]")
    console.print(db_status_tabel(fase))
    console.print()

    if not review_moment("Setup is klaar. Doorgaan naar stap 2 (Ingest)?"):
        stop_dashboard()


def stap_2_ingest(fase: str):
    stap_banner(2, "Masterdata laden", "blue")
    wat_gaan_we_doen(
        "We laden het Serax masterbestand in de database.\n\n"
        "Wat er gebeurt:\n"
        "  [cyan]1.[/cyan] Kolomnamen worden automatisch herkend (of opgeslagen mapping geladen)\n"
        "  [cyan]2.[/cyan] Je krijgt een preview van de koppeling ter goedkeuring\n"
        "  [cyan]3.[/cyan] EANs worden genormaliseerd (altijd 13-cijferig)\n"
        "  [cyan]4.[/cyan] Alle productvelden worden opgeslagen in Supabase\n"
        "  [cyan]5.[/cyan] Foto-URLs worden gekoppeld via de foto-export (optioneel)\n\n"
        "[bold red]Let op:[/bold red] Producten worden geladen met status 'raw' — nog geen verwerking."
    )

    leverancier = Prompt.ask(
        "[cyan]Leverancier (voor opgeslagen mapping)[/cyan]",
        default="serax",
    )
    masterdata = bestand_vragen("Pad naar masterdata Excel (.xlsx)", verplicht=True)
    fotos      = bestand_vragen("Pad naar foto-export Excel (.xlsx, optioneel)", verplicht=False)
    console.print()

    console.print("[cyan]Kolom-detectie en laden starten...[/cyan]")
    console.print(Rule(style="dim"))

    from execution.setup_masterdata import setup_masterdata
    setup_masterdata(masterdata, leverancier, fase, auto=False)

    # Foto-export koppelen als opgegeven
    if fotos:
        console.print()
        console.print("[cyan]Foto-export koppelen...[/cyan]")
        from execution.ingest import load_foto_export, get_supabase as _gsb
        foto_map = load_foto_export(fotos)
        if foto_map:
            sb2 = _gsb()
            for sku, urls in foto_map.items():
                sb2.table("seo_products").update(urls).eq("sku", sku).eq("fase", fase).execute()
            console.print(f"  [green]✓[/green] Foto-URLs gekoppeld voor {len(foto_map)} SKUs")

    console.print(Rule(style="dim"))
    console.print()

    # Toon resultaat
    sb = supabase()
    r  = sb.table("seo_products").select("sku,ean_shopify,giftbox,giftbox_qty,photo_packshot_1").eq("fase", fase).eq("status", "raw").limit(5).execute()
    if r.data:
        preview = Table(box=box.SIMPLE, header_style="bold", show_header=True)
        preview.add_column("SKU")
        preview.add_column("EAN (Shopify)")
        preview.add_column("Giftbox")
        preview.add_column("Foto?")
        for row in r.data:
            heeft_foto = "[green]✓[/green]" if row.get("photo_packshot_1") else "[dim]–[/dim]"
            preview.add_row(
                row.get("sku", ""),
                row.get("ean_shopify", ""),
                f"{row.get('giftbox','')} x{row.get('giftbox_qty','')}",
                heeft_foto,
            )

        count = len(sb.table("seo_products").select("id").eq("fase", fase).eq("status", "raw").execute().data or [])
        resultaat_panel(
            f"[green]{count} producten geladen[/green]\n\n"
            "Preview (eerste 5):",
            titel="Ingest Resultaat",
        )
        console.print(preview)
        console.print()
    else:
        resultaat_panel("[red]Geen producten geladen — check de foutmeldingen hierboven[/red]", ok=False)
        if not Confirm.ask("[yellow]Opnieuw proberen?[/yellow]"):
            stop_dashboard()
        stap_2_ingest(fase)
        return

    if not review_moment(
        "Controleer het aantal producten en de preview hierboven.\n"
        "  Kloppen de EANs? Zijn er foto's? Zijn er onverwachte waarschuwingen?\n\n"
        "  Doorgaan naar stap 3 (Matching)?"
    ):
        aanpassing_nodig(
            "Je kunt de ingest opnieuw draaien met een gecorrigeerd bestand.\n"
            "Verwijder eerst de geladen producten via:\n"
            "  python -c \"from dotenv import load_dotenv; load_dotenv(); from supabase import create_client; import os; "
            f"sb = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY')); "
            f"sb.table('seo_products').delete().eq('fase', '{fase}').eq('status', 'raw').execute(); print('Verwijderd')\""
        )
        stap_2_ingest(fase)


def stap_3_match(fase: str):
    stap_banner(3, "Match — Vergelijken met webshop", "blue")
    wat_gaan_we_doen(
        "We matchen elk product tegen de Shopify-webshop en het archief.\n\n"
        "Mogelijke uitkomsten:\n"
        "  [red]● Actief[/red]   — product staat nu actief op de webshop\n"
        "  [yellow]● Archief[/yellow]  — product stond eerder op de webshop (reactiveren)\n"
        "  [green]● Nieuw[/green]    — product is nog nooit aangemaakt in Shopify\n\n"
        "Matchlogica (strikt):\n"
        "  [cyan]1e prioriteit:[/cyan] exacte SKU-match\n"
        "  [cyan]Bij twijfel:[/cyan] het systeem stopt en vraagt jou om een beslissing\n\n"
        "[bold red]Elke twijfelgeval wordt aan jou voorgelegd — het systeem beslist dit NOOIT zelf.[/bold red]"
    )

    if not Confirm.ask("[bold]Match starten?[/bold]"):
        stop_dashboard()

    console.print()
    console.print(Rule(style="dim"))
    from execution.match import match_fase
    match_fase(fase)
    console.print(Rule(style="dim"))
    console.print()

    # Toon resultaat
    t1, t2 = fase_producten_status(fase)
    console.print("[bold]Status na matching:[/bold]")
    console.print(Columns([t1, t2]))
    console.print()

    # Check op eventuele twijfelgevallen die zijn overgeslagen
    sb = supabase()
    overgeslagen = sb.table("seo_products").select("sku,review_reden").eq("fase", fase).eq("status", "raw").execute()
    if overgeslagen.data:
        console.print(Panel(
            "\n".join(f"  [yellow]SKU {r['sku']}:[/yellow] {r.get('review_reden','')}" for r in overgeslagen.data[:10]),
            title=f"[yellow]{len(overgeslagen.data)} product(en) overgeslagen (status=raw)[/yellow]",
            border_style="yellow",
            padding=(0, 2),
        ))
        console.print()

    if not review_moment(
        "Controleer de matching-resultaten hierboven.\n"
        "  Kloppen de aantallen actief / archief / nieuw?\n\n"
        "  Doorgaan naar stap 4 (Transform)?"
    ):
        aanpassing_nodig(
            "Als de matching niet klopt:\n"
            "  - Update de Shopify-index: python execution/load_website_structure.py --webshop ...\n"
            "  - Reset de match: UPDATE seo_products SET status_shopify=NULL WHERE fase=... (via Supabase dashboard)\n"
            "  - Herstart stap 3"
        )
        stap_3_match(fase)


def stap_4_transform(fase: str, limit: int | None = None):
    stap_banner(4, "Transform — SOP-stappen uitvoeren", "blue")
    wat_gaan_we_doen(
        "We voeren alle SOP-stappen uit per product:\n\n"
        "  [cyan]1.[/cyan] Categorisering (op basis van mapping-tabel)\n"
        "  [cyan]2.[/cyan] Tags genereren (cat_{hoofd}, cat_{sub}, cat_{subsub}, structuur_fase)\n"
        "  [cyan]3.[/cyan] Materiaal vertalen naar Nederlands\n"
        "  [cyan]4.[/cyan] Kleur vertalen naar Nederlands (lamp-uitzondering inbegrepen)\n"
        "  [cyan]5.[/cyan] Producttitel opbouwen (set van X, lamp, OWL VASE, standaard)\n"
        "  [cyan]6.[/cyan] Prijs bepalen (stuk of giftbox)\n"
        "  [cyan]7.[/cyan] Meta description genereren via Claude\n"
        "  [cyan]8.[/cyan] Handle en decimalen opschonen\n\n"
        "[dim]Claude wordt alleen gebruikt voor meta descriptions en onbekende vertalingen.[/dim]\n"
        "[bold yellow]Twijfelgevallen (categorie onbekend, lamp-uitzondering) worden gerapporteerd.[/bold yellow]"
    )

    sb = supabase()
    raw_count = len(sb.table("seo_products").select("id").eq("fase", fase).eq("status", "raw").execute().data or [])
    te_doen = min(raw_count, limit) if limit else raw_count
    console.print(f"[cyan]Producten te verwerken: {te_doen}{f' (testrun, raw totaal: {raw_count})' if limit else ''}[/cyan]\n")

    if raw_count == 0:
        console.print("[yellow]Geen producten met status 'raw' gevonden. Zijn alle producten al verwerkt?[/yellow]")
        t1, _ = fase_producten_status(fase)
        console.print(t1)
        if not Confirm.ask("Toch doorgaan naar stap 5?"):
            stop_dashboard()
        return

    if not Confirm.ask(f"[bold]Transform starten voor {te_doen} producten? (Claude API credits worden gebruikt)[/bold]"):
        stop_dashboard()

    console.print()
    console.print(Rule(style="dim"))
    from execution.transform import transform
    transform(fase, limit)
    console.print(Rule(style="dim"))
    console.print()

    # Resultaat
    t1, t2 = fase_producten_status(fase)
    console.print("[bold]Status na transform:[/bold]")
    console.print(Columns([t1, t2]))
    console.print()

    # Review-items
    review_items = sb.table("seo_products").select("sku,review_reden,product_title_nl,kleur_nl,materiaal_nl").eq("fase", fase).eq("status", "review").execute()
    if review_items.data:
        rtabel = Table(box=box.ROUNDED, header_style="bold yellow", expand=True, show_lines=True)
        rtabel.add_column("SKU", width=20)
        rtabel.add_column("Reden", width=40)
        rtabel.add_column("Titel", width=40)
        for row in review_items.data[:15]:
            rtabel.add_row(
                row.get("sku", ""),
                row.get("review_reden", "") or "",
                row.get("product_title_nl", "") or "",
            )
        console.print(Panel(
            rtabel,
            title=f"[bold yellow]{len(review_items.data)} producten voor handmatige review[/bold yellow]",
            border_style="yellow",
        ))
        if len(review_items.data) > 15:
            console.print(f"[dim]... en {len(review_items.data) - 15} meer[/dim]")
        console.print()

    if not review_moment(
        "Controleer de resultaten:\n"
        "  - Zijn er onverwacht veel 'review' producten?\n"
        "  - Kloppen de twijfelgevallen hierboven?\n"
        "  - Zijn er nieuwe categorieën die eerst in Shopify aangemaakt moeten worden?\n\n"
        "  Doorgaan naar stap 5 (Validate)?"
    ):
        aanpassing_nodig(
            "Je kunt het volgende doen:\n"
            "  - Voeg ontbrekende categorieën toe aan seo_category_mapping\n"
            "  - Reset specifieke producten: UPDATE seo_products SET status='raw' WHERE sku='...'\n"
            "  - Herrun transform.py voor de gecorrigeerde producten"
        )
        stap_4_transform(fase)


def stap_5_validate(fase: str):
    stap_banner(5, "Validate — Kwaliteitscheck", "blue")
    wat_gaan_we_doen(
        "We controleren alle producten op kwaliteitsproblemen:\n\n"
        "  [cyan]✓[/cyan] Verplichte velden aanwezig (titel, EAN, categorie, prijs)\n"
        "  [cyan]✓[/cyan] Geen dubbele EANs\n"
        "  [cyan]✓[/cyan] Afmetingen decimaal correct (22.5 niet 22,50)\n"
        "  [cyan]✓[/cyan] Meta description max 160 tekens\n"
        "  [cyan]✓[/cyan] Prijs > €0\n"
        "  [cyan]✓[/cyan] Categorieën bestaan op de website\n"
        "  [cyan]✓[/cyan] Filterwaarden bestaan op de website\n\n"
        "[dim]Auto-fixes: afmetingen decimaal, meta description afkappen.[/dim]\n"
        f"[dim]Review-rapport wordt opgeslagen in: .tmp/review_fase{fase}.csv[/dim]"
    )

    if not Confirm.ask("[bold]Validatie starten?[/bold]"):
        stop_dashboard()

    console.print()
    console.print(Rule(style="dim"))
    from execution.validate import validate
    validate(fase)
    console.print(Rule(style="dim"))
    console.print()

    # Toon review-bestand als het bestaat
    review_path = Path(f".tmp/review_fase{fase}.csv")
    if review_path.exists():
        import csv
        with open(review_path, encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

        if rows:
            rtabel = Table(box=box.ROUNDED, header_style="bold yellow", expand=True)
            rtabel.add_column("SKU", width=20)
            rtabel.add_column("EAN", width=16)
            rtabel.add_column("Problemen", width=60)
            for row in rows[:20]:
                rtabel.add_row(row.get("sku",""), row.get("ean",""), row.get("issues",""))
            console.print(Panel(
                rtabel,
                title=f"[bold yellow]Review-rapport: {len(rows)} producten[/bold yellow]",
                border_style="yellow",
            ))
            if len(rows) > 20:
                console.print(f"[dim]... en {len(rows)-20} meer — open {review_path} voor volledig overzicht[/dim]")
            console.print()

    t1, _ = fase_producten_status(fase)
    console.print("[bold]Status na validatie:[/bold]")
    console.print(t1)
    console.print()

    if not review_moment(
        "Controleer het review-rapport.\n"
        "  - Zijn er blokkerende problemen (prijs 0, dubbele EAN, lege verplichte velden)?\n"
        "  - Zijn de waarschuwingen (ontbrekende foto's, afmetingen) acceptabel?\n"
        "  - Moeten er nog filterwaarden of categorieën aangemaakt worden in Shopify?\n\n"
        "  Doorgaan naar stap 6 (Export)?"
    ):
        aanpassing_nodig(
            "Corrigeer de problemen in het review-rapport.\n"
            f"  Review-bestand: {review_path}\n\n"
            "  Na correctie kun je validate.py opnieuw draaien:\n"
            "    python execution/validate.py --fase " + fase
        )
        stap_5_validate(fase)


def stap_6_export(fase: str):
    stap_banner(6, "Export — Shopify-importbestand genereren", "green")
    wat_gaan_we_doen(
        "We genereren één Excel-bestand met drie tabs:\n\n"
        "  [green]Tab 1 Shopify_Nieuw[/green]    — nieuwe producten (33 kolommen, zelfde format als template)\n"
        "  [yellow]Tab 2 Shopify_Archief[/yellow] — te reactiveren producten (incl. Product ID / Variant ID)\n"
        "  [cyan]Tab 3 Analyse[/cyan]        — samenvatting, nieuwe filterwaarden, review-items\n\n"
        "Kolomstructuur (per template):\n"
        "  Variant SKU · Product ID · Variant ID · Handle · Titel · Vendor · Type\n"
        "  EAN Code (tekst) · Prijs · Inkoopprijs · Beschrijving\n"
        "  Categorie (hoofd/sub/sub-sub) · Tags · Collectie · Designer · Materiaal · Kleur\n"
        "  Afmetingen · Meta description · Foto's (5x packshot + 5x lifestyle)"
    )

    sb = supabase()
    ready_count = len(sb.table("seo_products").select("id").eq("fase", fase).eq("status", "ready").execute().data or [])

    if ready_count == 0:
        console.print("[bold red]Geen producten met status 'ready'. Draai eerst validate.py.[/bold red]")
        stop_dashboard()

    console.print(f"[green]{ready_count} producten klaar voor export.[/green]\n")

    if not Confirm.ask(f"[bold]Export starten voor {ready_count} producten?[/bold]"):
        stop_dashboard()

    console.print()
    console.print(Rule(style="dim"))
    from execution.export_standaard import export_standaard
    bestand = export_standaard(fase, "./exports/")
    console.print(Rule(style="dim"))
    console.print()

    if bestand:
        pad = Path(bestand)
        grootte = f"{pad.stat().st_size / 1024:.1f} KB"

        resultaat_panel(
            f"[green]Export geslaagd![/green]\n\n"
            f"  Bestand:  [cyan]{bestand}[/cyan]\n"
            f"  Grootte:  {grootte}",
            titel="Export Klaar",
        )
        console.print()

    # Eindchecklist
    console.print(Panel(
        "[bold]Controleer voor je importeert in Shopify:[/bold]\n\n"
        "  [cyan]1.[/cyan] Open de Excel — check tab [bold]Analyse[/bold] voor nieuwe filterwaarden\n"
        "     → Aanmaken in Shopify VOOR je importeert!\n\n"
        "  [cyan]2.[/cyan] Tab [bold]Shopify_Nieuw[/bold]: kolom 8 (EAN Code) moet tekst zijn\n"
        "     → [bold]5400123456789[/bold] niet [bold]5.40E+12[/bold]\n\n"
        "  [cyan]3.[/cyan] Controleer 5 willekeurige titels tegen de SOP\n\n"
        "  [cyan]4.[/cyan] Afmetingen: [bold]22.5[/bold] niet [bold]22.50[/bold] of [bold]22,5[/bold]\n\n"
        "  [cyan]5.[/cyan] Importeer [bold]Shopify_Nieuw[/bold] eerst, daarna [bold]Shopify_Archief[/bold]\n\n"
        "[bold yellow]Pas na al deze checks importeren in Shopify![/bold yellow]",
        title="[bold yellow]Finale Checklist voor Import[/bold yellow]",
        border_style="yellow",
        padding=(1, 2),
    ))
    console.print()

    if review_moment("Zijn alle checks geslaagd? Klaar voor import in Shopify?"):
        console.print(Panel(
            f"[bold green]Batch fase {fase} is klaar![/bold green]\n\n"
            f"Bestand: [cyan]exports/{Path(bestand).name if bestand else ''}[/cyan]\n\n"
            "[dim]Succes met de import. Bij problemen kun je het dashboard opnieuw starten.[/dim]",
            border_style="green",
            padding=(1, 3),
        ))
    else:
        aanpassing_nodig(
            "Open de Excel en controleer de tabs.\n"
            "Druk op Enter om de export opnieuw te genereren."
        )
        stap_6_export(fase)


# ── Hoofd-orchestrator ────────────────────────────────────────────────────────

STAPPEN = ["setup", "ingest", "match", "transform", "validate", "export"]


def main():
    parser = argparse.ArgumentParser(description="Serax Product Onboarding Dashboard")
    parser.add_argument("--fase",   help="Fasecode (bijv. 3)")
    parser.add_argument("--vanaf",  choices=STAPPEN, help="Start vanaf deze stap")
    parser.add_argument("--limit",  type=int, default=None,
                        help="Beperk transform tot N producten (testrun, voorkomt grote Claude-rekening)")
    args = parser.parse_args()

    stap_0_welkom_en_check()

    # Fase bepalen
    fase = args.fase
    if not fase:
        console.print()
        fase = Prompt.ask("[bold cyan]Voor welke fase verwerk je producten?[/bold cyan] (bijv. 3)")
    console.print(f"\n[bold]Fase: {fase}[/bold]\n")

    # Startpunt
    vanaf = args.vanaf or "setup"
    stap_idx = STAPPEN.index(vanaf)

    # Pipeline uitvoeren
    stappen = [
        ("setup",     lambda: stap_1_setup(fase)),
        ("ingest",    lambda: stap_2_ingest(fase)),
        ("match",     lambda: stap_3_match(fase)),
        ("transform", lambda: stap_4_transform(fase, args.limit)),
        ("validate",  lambda: stap_5_validate(fase)),
        ("export",    lambda: stap_6_export(fase)),
    ]

    for naam, stap_fn in stappen[stap_idx:]:
        stap_fn()


if __name__ == "__main__":
    main()
