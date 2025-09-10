"""
Polygon.io Russell 3000 Historical Data Downloader
Downloads 1-minute OHLCV data for Russell 3000 stocks from 2019-2025
Optimized for the $29/month Stocks Starter plan with unlimited API calls

for other users:
change all_tickers in get_russell3000_tickers function to the tickers you want the data for
change DATA_PATH in main function to the place where you want the .csv files
change API_KEY to my API_KEY (which ill send separately)
change start date and end date in download_ticker_data and generate_date_chunks functions to change the date range of 1min data
    (dont think polygon offers 1min data before 09/08/20 though)
"""

import os
import time
import json
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import threading
from typing import List, Dict, Optional, Tuple

class PolygonDataDownloader:
    def __init__(self, api_key: str, base_path: str = "D:/Russell_3000_1min_Data_01-01-19_08-29-25"):
        """
        Initialize the Polygon.io data downloader
        
        Args:
            api_key: Your Polygon.io API key
            base_path: Base directory to store downloaded data
        """
        self.api_key = api_key
        self.base_url = "https://api.polygon.io"
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # CSV files go directly in the main folder
        # Create subdirectories for logs and progress tracking only
        self.logs_path = self.base_path / "logs"
        self.progress_path = self.base_path / "progress"
        
        for path in [self.logs_path, self.progress_path]:
            path.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        self._setup_logging()
        
        # Progress tracking - track by ticker instead of chunks
        self.lock = threading.Lock()
        self.progress_file = self.progress_path / "download_progress.json"
        self.completed_tickers = self._load_progress()
        
        # Statistics
        self.stats = {
            'total_api_calls': 0,
            'successful_calls': 0,
            'failed_calls': 0,
            'total_data_points': 0,
            'start_time': None
        }
        
    def _setup_logging(self):
        """Setup logging configuration"""
        log_file = self.logs_path / f"download_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def _load_progress(self) -> set:
        """Load previously completed tickers to resume downloads"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    progress = json.load(f)
                    return set(progress.get('completed_tickers', []))
            except Exception as e:
                self.logger.warning(f"Could not load progress file: {e}")
        return set()
    
    def _save_progress(self, ticker: str):
        """Save progress for resume capability"""
        with self.lock:
            self.completed_tickers.add(ticker)
            # Create a JSON-safe copy of stats
            safe_stats = self.stats.copy()
            if safe_stats['start_time']:
                safe_stats['start_time'] = safe_stats['start_time'].isoformat()
            
            progress_data = {
                'completed_tickers': list(self.completed_tickers),
                'last_updated': datetime.now().isoformat(),
                'stats': safe_stats
            }
            with open(self.progress_file, 'w') as f:
                json.dump(progress_data, f, indent=2)
    
    def get_russell3000_tickers(self) -> List[str]:
        """
        Fetch current Russell 3000 constituent tickers from Polygon.io
        Returns list of ticker symbols
        """
        self.logger.info("Fetching Russell 3000 tickers...")
        
        # Polygon.io doesn't have a direct Russell 3000 endpoint
        # We'll get all US stock tickers and filter by market cap/volume
        # This is an approximation but should capture most R3000 stocks
        
        # Russell 3000 ticker symbols (hard copied! dont ask lol)
        
        all_tickers = [

        # A stocks (complete)
        "A", "AA", "AAL", "AAMI", "AAOI", "AAON", "AAP", "AAPL", "AARD", "AAT", "ABAT", "ABBV", "ABCB", "ABEO", "ABG", "ABM", "ABNB", "ABR", "ABSI", "ABT",
        "ABUS", "ACA", "ACAD", "ACCO", "ACDC", "ACEL", "ACGL", "ACHC", "ACHR", "ACI", "ACIC", "ACIW", "ACLS", "ACLX", "ACM", "ACMR", "ACN", "ACNB", "ACNT", "ACR",
        "ACRE", "ACRS", "ACT", "ACTG", "ACTU", "ACU", "ACVA", "ADAM", "ADBE", "ADC", "ADCT", "ADEA", "ADI", "ADM", "ADMA", "ADNT", "ADP", "ADPT", "ADSK", "ADT",
        "ADTN", "ADUS", "ADV", "AEBI", "AEE", "AEHR", "AEIS", "AEO", "AEP", "AES", "AESI", "AEVA", "AEYE", "AFCG", "AFG", "AFL", "AFRI", "AFRM", "AGCO", "AGIO",
        "AGL", "AGM", "AGNC", "AGO", "AGX", "AGYS", "AHCO", "AHH", "AHR", "AI", "AIG", "AIN", "AIOT", "AIP", "AIR", "AIRJ", "AIRS", "AISP", "AIT", "AIV",
        "AIZ", "AJG", "AKAM", "AKBA", "AKR", "AKRO", "AL", "ALAB", "ALB", "ALCO", "ALDX", "ALE", "ALEC", "ALEX", "ALG", "ALGM", "ALGN", "ALGT", "ALHC", "ALIT",
        "ALK", "ALKS", "ALKT", "ALL", "ALLE", "ALLO", "ALLY", "ALMS", "ALMU", "ALNT", "ALNY", "ALRM", "ALRS", "ALSN", "ALT", "ALTG", "ALTI", "ALX", "AM", "AMAL",
        "AMAT", "AMBA", "AMBC", "AMBP", "AMC", "AMCR", "AMCX", "AMD", "AME", "AMG", "AMGN", "AMH", "AMKR", "AMLX", "AMN", "AMP", "AMPH", "AMPL", "AMPX", "AMR",
        "AMRC", "AMRK", "AMRX", "AMSC", "AMSF", "AMT", "AMTB", "AMTM", "AMWD", "AMZN", "AN", "ANAB", "ANDE", "ANET", "ANF", "ANGI", "ANGO", "ANIK", "ANIP", "ANNX",
        "AOMR", "AON", "AORT", "AOS", "AOSL", "AOUT", "APA", "APAM", "APD", "APEI", "APG", "APGE", "APH", "APLD", "APLE", "APLS", "APO", "APOG", "APP", "APPF",
        "APPN", "APPS", "APTV", "AQST", "AR", "ARAY", "ARCB", "ARCT", "ARDT", "ARDX", "ARE", "AREN", "ARES", "ARHS", "ARI", "ARIS", "ARKO", "ARL", "ARLO", "ARMK",
        "AROC", "AROW", "ARQ", "ARQT", "ARR", "ARRY", "ARVN", "ARW", "ARWR", "AS", "ASAN", "ASB", "ASC", "ASGN", "ASH", "ASIX", "ASLE", "ASO", "ASPI", "ASPN",
        "ASTE", "ASTH", "ASTS", "ASUR", "ATEC", "ATEN", "ATEX", "ATGE", "ATI", "ATKR", "ATLC", "ATLN", "ATLO", "ATMU", "ATNI", "ATO", "ATOM", "ATR", "ATRC", "ATRO",
        "ATUS", "ATXS", "ATYR", "AU", "AUB", "AUPH", "AUR", "AURA", "AVA", "AVAH", "AVAV", "AVB", "AVBP", "AVD", "AVDL", "AVDX", "AVGO", "AVIR", "AVNS", "AVNT",
        "AVNW", "AVO", "AVPT", "AVR", "AVT", "AVTR", "AVXL", "AVY", "AWI", "AWK", "AWR", "AX", "AXGN", "AXL", "AXON", "AXP", "AXS", "AXSM", "AXTA", "AYI",
        "AZO", "AZTA", "AZZ",
        
        # B stocks (complete)
        "BA", "BAC", "BAH", "BALL", "BALY", "BAM", "BANC", "BAND", "BANF", "BANR", "BARK", "BASE", "BATRA", "BATRK", "BAX", "BBAI", "BBBY", "BBCP", "BBIO", "BBNX",
        "BBSI", "BBT", "BBUC", "BBW", "BBWI", "BBY", "BC", "BCAL", "BCAX", "BCBP", "BCC", "BCML", "BCO", "BCPC", "BCRX", "BDC", "BDN", "BDX", "BE", "BEAM",
        "BEEP", "BELFA", "BELFB", "BEN", "BEPC", "BETR", "BF.A", "BF.B", "BFAM", "BFC", "BFH", "BFIN", "BFLY", "BFS", "BFST", "BG", "BGC", "BGS", "BH", "BHB",
        "BHE", "BHF", "BHR", "BHRB", "BHVN", "BIIB", "BILL", "BIO", "BIOA", "BIPC", "BIRK", "BJ", "BJRI", "BK", "BKD", "BKE", "BKH", "BKKT", "BKNG", "BKR",
        "BKSY", "BKTI", "BKU", "BKV", "BL", "BLBD", "BLD", "BLDR", "BLFS", "BLFY", "BLK", "BLKB", "BLMN", "BLND", "BLX", "BLZE", "BMBL", "BMI", "BMRC", "BMRN",
        "BMY", "BNED", "BNL", "BNTC", "BOC", "BOH", "BOKF", "BOOM", "BOOT", "BORR", "BOW", "BOX", "BPOP", "BPRN", "BR", "BRBR", "BRBS", "BRCC", "BRK.B", "BRKR",
        "BRO", "BROS", "BRSL", "BRSP", "BRT", "BRX", "BRY", "BRZE", "BSET", "BSRR", "BSVN", "BSX", "BSY", "BTBT", "BTDR", "BTMD", "BTSG", "BTU", "BUR", "BURL",
        "BUSE", "BV", "BVFL", "BVS", "BWA", "BWB", "BWFG", "BWIN", "BWMN", "BWXT", "BX", "BXC", "BXMT", "BXP", "BY", "BYD", "BYND", "BYRN", "BZH",
        
        # C stocks (complete)
        "C", "CABO", "CAC", "CACC", "CACI", "CADE", "CADL", "CAG", "CAH", "CAKE", "CAL", "CALM", "CALX", "CAPR", "CAR", "CARE", "CARG", "CARR", "CARS", "CART",
        "CASH", "CASS", "CASY", "CAT", "CATX", "CATY", "CAVA", "CB", "CBAN", "CBFV", "CBL", "CBLL", "CBNA", "CBNK", "CBOE", "CBRE", "CBRL", "CBSH", "CBT", "CBU",
        "CBZ", "CC", "CCB", "CCBG", "CCCS", "CCI", "CCK", "CCL", "CCNE", "CCOI", "CCRD", "CCRN", "CCS", "CCSI", "CDE", "CDNA", "CDNS", "CDP", "CDRE", "CDTX",
        "CDW", "CDXS", "CDZI", "CE", "CECO", "CEG", "CELC", "CELH", "CENT", "CENTA", "CENX", "CERS", "CERT", "CEVA", "CF", "CFBK", "CFFI", "CFFN", "CFG", "CFLT",
        "CFR", "CG", "CGEM", "CGNX", "CGON", "CHCO", "CHCT", "CHD", "CHDN", "CHE", "CHEF", "CHH", "CHMG", "CHRD", "CHRS", "CHRW", "CHTR", "CHWY", "CI", "CIA",
        "CIEN", "CIFR", "CIM", "CINF", "CIO", "CIVB", "CIVI", "CIX", "CL", "CLAR", "CLB", "CLBK", "CLDT", "CLDX", "CLF", "CLFD", "CLH", "CLMB", "CLMT",
        "CLNE", "CLOV", "CLPR", "CLPT", "CLSK", "CLVT", "CLW", "CLX", "CMA", "CMC", "CMCL", "CMCO", "CMCSA", "CMDB", "CME", "CMG", "CMI", "CMP", "CMPO",
        "CMPR", "CMPX", "CMRC", "CMRE", "CMS", "CMT", "CMTG", "CNA", "CNC", "CNDT", "CNH", "CNK", "CNM", "CNMD", "CNNE", "CNO", "CNOB", "CNP", "CNR", "CNS",
        "CNX", "CNXC", "CNXN", "COCO", "CODI", "COF", "COFS", "COGT", "COHR", "COHU", "COIN", "COKE", "COLB", "COLD", "COLL", "COLM", "COMM", "COMP", "CON", "COO",
        "COOK", "COOP", "COP", "COR", "CORT", "CORZ", "COST", "COTY", "COUR", "CPAY", "CPB", "CPF", "CPK", "CPNG", "CPRI", "CPRT", "CPRX", "CPS", "CPSS", "CPT",
        "CR", "CRAI", "CRBG", "CRC", "CRCT", "CRD.A", "CRDF", "CRDO", "CRGY", "CRH", "CRI", "CRK", "CRL", "CRM", "CRMD", "CRML", "CRMT", "CRNC", "CRNX", "CROX",
        "CRS", "CRSP", "CRSR", "CRUS", "CRVL", "CRVS", "CRWD", "CSCO", "CSGP", "CSGS", "CSL", "CSPI", "CSR", "CSTL", "CSTM", "CSV", "CSW", "CSX", "CTAS", "CTBI",
        "CTEV", "CTGO", "CTKB", "CTLP", "CTO", "CTOS", "CTRA", "CTRE", "CTRI", "CTRN", "CTS", "CTSH", "CTVA", "CUBE", "CUBI", "CURB", "CURI", "CURV", "CUZ", "CVBF",
        "CVCO", "CVGW", "CVI", "CVLG", "CVLT", "CVNA", "CVRX", "CVS", "CVX", "CW", "CWAN", "CWBC", "CWCO", "CWEN", "CWEN.A", "CWH", "CWK", "CWST", "CWT", "CXDO",
        "CXM", "CXT", "CXW", "CYH", "CYRX", "CYTK", "CZFS", "CZNC", "CZR", "CZWI",

        # D stocks (complete)
        "D", "DAKT", "DAL", "DAN", "DAR", "DASH", "DAVE", "DAWN", "DAY", "DBD", "DBI", "DBRG", "DBX", "DC", "DCGO", "DCI", "DCO", "DCOM", "DCTH", "DD",
        "DDD", "DDOG", "DDS", "DE", "DEA", "DEC", "DECK", "DEI", "DELL", "DENN", "DERM", "DFH", "DFIN", "DG", "DGICA", "DGII", "DGX", "DH", "DHC", "DHI",
        "DHIL", "DHR", "DHT", "DIN", "DINO", "DIOD", "DIS", "DJCO", "DJT", "DK", "DKNG", "DKS", "DLB", "DLR", "DLTR", "DLX", "DMAC", "DMRC", "DNA", "DNLI",
        "DNOW", "DNTH", "DNUT", "DOC", "DOCN", "DOCS", "DOCU", "DOLE", "DOMO", "DORM", "DOUG", "DOV", "DOW", "DOX", "DPZ", "DRH", "DRI", "DRS", "DRUG", "DRVN",
        "DSGN", "DSGR", "DSP", "DT", "DTE", "DTM", "DUK", "DUOL", "DV", "DVA", "DVAX", "DVN", "DX", "DXC", "DXCM", "DXPE", "DY", "DYN",
        
        # E stocks (complete)
        "EA", "EAT", "EB", "EBAY", "EBC", "EBF", "EBMT", "EBS", "ECBK", "ECG", "ECL", "ECPG", "ECVT", "ED", "EDIT", "EE", "EEFT", "EEX", "EFC", "EFSC",
        "EFSI", "EFX", "EG", "EGAN", "EGBN", "EGHT", "EGP", "EGY", "EHAB", "EHC", "EHTH", "EIG", "EIX", "EL", "ELA", "ELAN", "ELDN", "ELF", "ELMD", "ELME",
        "ELS", "ELV", "ELVN", "EMBC", "EME", "EML", "EMN", "EMR", "ENOV", "ENPH", "ENR", "ENS", "ENSG", "ENTA", "ENTG", "ENVA", "ENVX", "EOG", "EOLS", "EOSE",
        "EP", "EPAC", "EPAM", "EPC", "EPM", "EPR", "EPRT", "EPSN", "EQBK", "EQH", "EQIX", "EQR", "EQT", "ERAS", "ERII", "ES", "ESAB", "ESCA", "ESE", "ESI",
        "ESNT", "ESOA", "ESPR", "ESQ", "ESRT", "ESS", "ESTC", "ETD", "ETN", "ETNB", "ETON", "ETR", "ETSY", "EU", "EVC", "EVCM", "EVER", "EVEX", "EVGO", "EVH",
        "EVI", "EVLV", "EVR", "EVRG", "EVTC", "EW", "EWBC", "EWCZ", "EWTX", "EXAS", "EXC", "EXE", "EXEL", "EXFY", "EXLS", "EXP", "EXPD", "EXPE", "EXPI", "EXPO",
        "EXR", "EXTR", "EYE", "EYPT",
        
        # F stocks (complete)
        "F", "FA", "FAF", "FANG", "FAST", "FATE", "FBIN", "FBIZ", "FBK", "FBLA", "FBNC", "FBP", "FBRT", "FC", "FCAP", "FCBC", "FCCO", "FCF", "FCFS", "FCN",
        "FCNCA", "FCPT", "FCX", "FDBC", "FDMT", "FDP", "FDS", "FDX", "FE", "FEIM", "FELE", "FENC", "FERG", "FET", "FF", "FFAI", "FFBC", "FFIC", "FFIN", "FFIV",
        "FFWM", "FG", "FHB", "FHN", "FHTX", "FI", "FIBK", "FICO", "FIGS", "FIHL", "FINW", "FIP", "FIS", "FISI", "FITB", "FIVE", "FIVN", "FIX", "FIZZ", "FL",
        "FLEX", "FLG", "FLGT", "FLNC", "FLNG", "FLO", "FLOC", "FLR", "FLS", "FLUT", "FLWS", "FLXS", "FLYW", "FLYYQ", "FMAO", "FMBH", "FMC", "FMNB", "FN", "FNB",
        "FND", "FNF", "FNKO", "FNLC", "FNWD", "FOA", "FOLD", "FOR", "FORM", "FORR", "FOUR", "FOX", "FOXA", "FOXF", "FPI", "FR", "FRAF", "FRBA", "FRD", "FRGE",
        "FRHC", "FRME", "FRPH", "FRPT", "FRSH", "FRST", "FRT", "FSBC", "FSBW", "FSFG", "FSLR", "FSLY", "FSP", "FSS", "FSTR", "FSUN", "FTAI", "FTDR", "FTI", "FTK",
        "FTLF", "FTNT", "FTRE", "FTV", "FUBO", "FUL", "FULC", "FULT", "FUN", "FUNC", "FVCB", "FVR", "FWONA", "FWONK", "FWRD", "FWRG", "FXNC", "FYBR",
        
        # G stocks (complete)
        "G", "GABC", "GAIA", "GAMB", "GAP", "GATX", "GBCI", "GBFH", "GBTG", "GBX", "GCBC", "GCI", "GCMG", "GCO", "GCT", "GD", "GDDY", "GDEN", "GDOT", "GDYN",
        "GE", "GEF", "GEF.B", "GEHC", "GEN", "GENC", "GENI", "GEO", "GERN", "GETY", "GEV", "GEVO", "GFF", "GFS", "GGG", "GH", "GHC", "GHM", "GIC", "GIII",
        "GILD", "GIS", "GKOS", "GL", "GLDD", "GLIBA", "GLIBK", "GLNG", "GLOB", "GLPI", "GLRE", "GLSI", "GLUE", "GLW", "GM", "GME", "GMED", "GMGI", "GMRE", "GMS",
        "GNE", "GNK", "GNL", "GNRC", "GNTX", "GNTY", "GNW", "GO", "GOCO", "GOGO", "GOLF", "GOOD", "GOOG", "GOOGL", "GOSS", "GPC", "GPI", "GPK", "GPN", "GPOR",
        "GPRE", "GRAL", "GRBK", "GRC", "GRDN", "GRMN", "GRND", "GRNT", "GRPN", "GS", "GSAT", "GSBC", "GSHD", "GSM", "GT", "GTES", "GTLB", "GTLS", "GTM", "GTN",
        "GTX", "GTY", "GVA", "GWRE", "GWRS", "GWW", "GXO", "GYRE",

        # H stocks (complete)
        "H", "HAE", "HAFC", "HAIN", "HAL", "HALO", "HAS", "HASI", "HAYW", "HBAN", "HBB", "HBCP", "HBI", "HBNC", "HBT", "HCA", "HCAT", "HCC", "HCI", "HCKT",
        "HCSG", "HD", "HDSN", "HE", "HEI", "HEI.A", "HELE", "HFFG", "HFWA", "HG", "HGV", "HHH", "HI", "HIFS", "HIG", "HII", "HIMS", "HIPO", "HIW", "HL",
        "HLF", "HLI", "HLIO", "HLIT", "HLLY", "HLMN", "HLNE", "HLT", "HLX", "HMN", "HNI", "HNRG", "HNST", "HNVR", "HOG", "HOLX", "HOMB", "HON", "HONE", "HOOD",
        "HOPE", "HOUS", "HOV", "HP", "HPE", "HPK", "HPP", "HPQ", "HQI", "HQY", "HR", "HRB", "HRI", "HRL", "HRMY", "HROW", "HRTG", "HRTX", "HSHP", "HSIC",
        "HSII", "HST", "HSTM", "HSY", "HTB", "HTBK", "HTH", "HTLD", "HTO", "HTZ", "HUBB", "HUBG", "HUBS", "HUM", "HUMA", "HUN", "HURA", "HURN", "HUT", "HVT",
        "HWBK", "HWC", "HWKN", "HWM", "HXL", "HY", "HYLN", "HZO",
        
        # I stocks (complete)
        "IAC", "IART", "IAS", "IBCP", "IBEX", "IBKR", "IBM", "IBOC", "IBP", "IBRX", "IBTA", "ICE", "ICFI", "ICHR", "ICUI", "IDA", "IDCC", "IDR", "IDT", "IDXX",
        "IDYA", "IE", "IESC", "IEX", "IFF", "IHRT", "III", "IIIN", "IIIV", "IIPR", "IKT", "ILLR", "ILMN", "ILPT", "IMAX", "IMKTA", "IMMR", "IMNM", "IMVT", "IMXI",
        "INBK", "INBX", "INCY", "INDB", "INDI", "INDV", "INFA", "INGM", "INGN", "INGR", "INMB", "INN", "INNV", "INOD", "INR", "INSE", "INSG", "INSM", "INSP", "INSW",
        "INTA", "INTC", "INTU", "INVA", "INVH", "INVX", "IONQ", "IONS", "IOSP", "IOT", "IOVA", "IP", "IPAR", "IPG", "IPGP", "IPI", "IQV", "IR", "IRDM", "IRM",
        "IRMD", "IRON", "IRT", "IRTC", "IRWD", "ISPR", "ISRG", "ISTR", "IT", "ITGR", "ITIC", "ITRI", "ITT", "ITW", "IVR", "IVT", "IVZ",
        
        # J stocks (complete)
        "J", "JACK", "JAKK", "JAMF", "JANX", "JAZZ", "JBGS", "JBHT", "JBI", "JBIO", "JBL", "JBLU", "JBSS", "JBTM", "JCI", "JEF", "JELD", "JHG", "JHX", "JILL",
        "JJSF", "JKHY", "JLL", "JMSB", "JNJ", "JOBY", "JOE", "JOUT", "JPM", "JRVR", "JXN", "JYNT",
        
        # K stocks (complete)
        "K", "KAI", "KALU", "KALV", "KAR", "KBH", "KBR", "KD", "KDP", "KE", "KELYA", "KEX", "KEY", "KEYS", "KFRC", "KFS", "KFY", "KG", "KGEI", "KGS",
        "KHC", "KIDS", "KIM", "KINS", "KKR", "KLAC", "KLC", "KLG", "KLIC", "KLTR", "KMB", "KMI", "KMPR", "KMT", "KMTS", "KMX", "KN", "KNF", "KNSL", "KNTK",
        "KNX", "KO", "KOD", "KODK", "KOP", "KOPN", "KOS", "KR", "KRC", "KREF", "KRG", "KRMD", "KRMN", "KRNY", "KRO", "KROS", "KRRO", "KRT", "KRUS", "KRYS",
        "KSS", "KTB", "KTOS", "KULR", "KURA", "KVUE", "KW", "KWR", "KYMR",

        # L stocks (complete)
        "L", "LAB", "LAD", "LADR", "LAKE", "LAMR", "LAND", "LARK", "LASR", "LAUR", "LAW", "LAZ", "LAZR", "LBRDA", "LBRDK", "LBRT", "LBTYA", "LBTYK", "LC", "LCID",
        "LCII", "LCNB", "LDI", "LDOS", "LE", "LEA", "LECO", "LEG", "LEGH", "LEN", "LEN.B", "LENZ", "LEU", "LFCR", "LFMD", "LFST", "LFT", "LFUS", "LFVN", "LGIH",
        "LGND", "LH", "LHX", "LIF", "LII", "LILA", "LILAK", "LIN", "LINC", "LIND", "LINE", "LION", "LITE", "LIVN", "LKFN", "LKQ", "LLY", "LLYVA", "LLYVK", "LMAT",
        "LMB", "LMND", "LMNR", "LMT", "LNC", "LNG", "LNKB", "LNN", "LNSR", "LNT", "LNTH", "LNW", "LOAR", "LOB", "LOCO", "LOPE", "LOVE", "LOW", "LPA", "LPG",
        "LPLA", "LPRO", "LPX", "LQDA", "LQDT", "LRCX", "LRMR", "LRN", "LSCC", "LSTR", "LTBR", "LTC", "LTH", "LUCD", "LULU", "LUMN", "LUNG", "LUNR", "LUV",
        "LVS", "LVWR", "LW", "LWAY", "LXEO", "LXFR", "LXP", "LXU", "LYB", "LYFT", "LYTS", "LYV", "LZ", "LZB", "LZM",
        
        # M stocks (complete)
        "M", "MA", "MAA", "MAC", "MAGN", "MAMA", "MAN", "MANH", "MAPS", "MAR", "MARA", "MAS", "MASI", "MASS", "MAT", "MATV", "MATW", "MATX", "MAX", "MAZE",
        "MBC", "MBCN", "MBI", "MBIN", "MBUU", "MBWM", "MBX", "MC", "MCB", "MCBS", "MCD", "MCFT", "MCHB", "MCHP", "MCK", "MCO", "MCRI", "MCS", "MCW", "MCY",
        "MD", "MDB", "MDGL", "MDLZ", "MDT", "MDU", "MDV", "MDWD", "MDXG", "MEC", "MED", "MEDP", "MEG", "MEI", "MET", "META", "METC", "MFA", "MFH", "MFIN",
        "MG", "MGEE", "MGM", "MGNI", "MGPI", "MGRC", "MGTX", "MGY", "MHK", "MHO", "MIDD", "MIR", "MIRM", "MITK", "MITT", "MKC", "MKL", "MKSI", "MKTW", "MKTX",
        "MLAB", "MLI", "MLKN", "MLM", "MLNK", "MLP", "MLR", "MLYS", "MMC", "MMI", "MMM", "MMS", "MMSI", "MNKD", "MNMD", "MNPR", "MNRO", "MNSB", "MNST", "MNTK",
        "MO", "MOD", "MODG", "MOFG", "MOG.A", "MOH", "MORN", "MOS", "MOV", "MP", "MPAA", "MPB", "MPC", "MPTI", "MPW", "MPWR", "MPX", "MQ", "MRBK", "MRC",
        "MRCY", "MRK", "MRNA", "MRP", "MRTN", "MRVI", "MRVL", "MRX", "MS", "MSA", "MSBI", "MSCI", "MSEX", "MSFT", "MSGE", "MSGS", "MSI", "MSM", "MSTR", "MTAL",
        "MTB", "MTCH", "MTD", "MTDR", "MTG", "MTH", "MTN", "MTRN", "MTRX", "MTSI", "MTSR", "MTUS", "MTW", "MTX", "MTZ", "MU", "MUR", "MUSA", "MVBF", "MVIS",
        "MVST", "MWA", "MXCT", "MXL", "MYE", "MYFW", "MYGN", "MYO", "MYPS", "MYRG", "MZTI",
        
        # N stocks (complete)
        "NABL", "NAGE", "NAT", "NATH", "NATL", "NATR", "NAVI", "NB", "NBBK", "NBHC", "NBIX", "NBN", "NBR", "NBTB", "NC", "NCLH", "NCMI", "NCNO", "NDAQ", "NDSN",
        "NE", "NECB", "NEE", "NEM", "NEO", "NEOG", "NEON", "NESR", "NET", "NEU", "NEWT", "NEXN", "NEXT", "NFBK", "NFE", "NFG", "NFLX", "NG", "NGNE", "NGS",
        "NGVC", "NGVT", "NHC", "NHI", "NI", "NIC", "NJR", "NKE", "NKSH", "NKTX", "NL", "NLOP", "NLY", "NMAX", "NMIH", "NMRK", "NN", "NNE", "NNI", "NNN",
        "NNOX", "NOC", "NODK", "NOG", "NOV", "NOVT", "NOW", "NPB", "NPCE", "NPK", "NPKI", "NPO", "NPWR", "NRC", "NRDS", "NRDY", "NREF", "NRG", "NRIM", "NRIX",
        "NSA", "NSC", "NSIT", "NSP", "NSSC", "NTAP", "NTB", "NTCT", "NTGR", "NTLA", "NTNX", "NTRA", "NTRS", "NTST", "NU", "NUE", "NUS", "NUTX", "NUVB",
        "NUVL", "NVAX", "NVCR", "NVCT", "NVDA", "NVEC", "NVGS", "NVR", "NVRI", "NVST", "NVT", "NVTS", "NWBI", "NWE", "NWFL", "NWL", "NWN", "NWPX", "NWS",
        "NWSA", "NX", "NXDR", "NXDT", "NXRT", "NXST", "NXT", "NXXT", "NYT",
        
        # O stocks (complete)
        "O", "OABI", "OBK", "OBT", "OC", "OCFC", "OCUL", "ODC", "ODFL", "ODP", "OEC", "OFG", "OFIX", "OFLX", "OGE", "OGN", "OGS", "OHI", "OI", "OII",
        "OIS", "OKE", "OKLO", "OKTA", "OLED", "OLLI", "OLMA", "OLN", "OLO", "OLP", "OLPX", "OM", "OMC", "OMCL", "OMER", "OMF", "OMI", "ON", "ONB", "ONEW",
        "ONIT", "ONON", "ONTF", "ONTO", "OOMA", "OPAL", "OPBK", "OPCH", "OPFI", "OPK", "OPOF", "OPRT", "OPRX", "ORA", "ORC", "ORCL", "ORGO", "ORI", "ORIC", "ORKA",
        "ORLY", "ORN", "ORRF", "OSBC", "OSCR", "OSIS", "OSK", "OSPN", "OSUR", "OSW", "OTIS", "OTTR", "OUST", "OUT", "OVBC", "OVLY", "OVV", "OWL", "OXM", "OXY",
        "OZK",
        
        # P stocks (complete)
        "PACB", "PACK", "PACS", "PAG", "PAGS", "PAHC", "PAL", "PAMT", "PANL", "PANW", "PAR", "PARR", "PATH", "PATK", "PAX", "PAYC", "PAYO", "PAYS", "PAYX", "PB",
        "PBF", "PBFS", "PBH", "PBI", "PBPB", "PBYI", "PCAR", "PCB", "PCG", "PCH", "PCOR", "PCRX", "PCT", "PCTY", "PCVX", "PCYO", "PD", "PDEX", "PDFS", "PDLB",
        "PDM", "PDYN", "PEB", "PEBK", "PEBO", "PECO", "PEG", "PEGA", "PEN", "PENG", "PENN", "PEP", "PESI", "PFBC", "PFE", "PFG", "PFGC", "PFIS", "PFS", "PFSI",
        "PG", "PGC", "PGEN", "PGNY", "PGR", "PGRE", "PGY", "PH", "PHAT", "PHIN", "PHLT", "PHM", "PHR", "PI", "PII", "PINC", "PINE", "PINS", "PIPR", "PJT",
        "PK", "PKBK", "PKE", "PKG", "PKOH", "PKST", "PL", "PLAB", "PLAY", "PLBC", "PLD", "PLMR", "PLNT", "PLOW", "PLPC", "PLSE", "PLTK", "PLTR", "PLUG", "PLUS",
        "PLX", "PLXS", "PLYM", "PM", "PMT", "PMTS", "PNBK", "PNC", "PNFP", "PNR", "PNRG", "PNTG", "PNW", "PODD", "POOL", "POR", "POST", "POWI", "POWL", "POWW",
        "PPC", "PPG", "PPL", "PPTA", "PR", "PRA", "PRAA", "PRAX", "PRCH", "PRCT", "PRDO", "PRG", "PRGO", "PRGS", "PRI", "PRIM", "PRK", "PRKS", "PRLB", "PRM",
        "PRMB", "PRME", "PRO", "PROP", "PRSU", "PRTA", "PRTH", "PRU", "PRVA", "PSA", "PSFE", "PSIX", "PSMT", "PSN", "PSNL", "PSTG", "PSTL", "PSX", "PTC", "PTCT",
        "PTEN", "PTGX", "PTLO", "PTON", "PUBM", "PUMP", "PVBC", "PVH", "PVLA", "PWP", "PWR", "PX", "PYPL", "PZZA",

        # Q stocks (complete)
        "QBTS", "QCOM", "QCRH", "QDEL", "QGEN", "QLYS", "QNST", "QRVO", "QS", "QSI", "QSR", "QTRX", "QTWO", "QUAD", "QUBT", "QXO",
        
        # R stocks (complete)
        "R", "RAL", "RAMP", "RAPP", "RARE", "RBA", "RBB", "RBBN", "RBC", "RBCAA", "RBKB", "RBLX", "RBRK", "RC", "RCAT", "RCEL", "RCKT", "RCKY", "RCL", "RCMT",
        "RCUS", "RDDT", "RDN", "RDNT", "RDVT", "RDW", "REAL", "REAX", "REFI", "REG", "REGN", "RELL", "RELY", "REPL", "REPX", "RES", "REVG", "REX", "REXR", "REYN",
        "REZI", "RF", "RGA", "RGCO", "RGEN", "RGLD", "RGNX", "RGP", "RGR", "RGTI", "RH", "RHI", "RHLD", "RHP", "RICK", "RIG", "RIGL", "RIOT", "RITM", "RIVN",
        "RJF", "RKLB", "RKT", "RL", "RLAY", "RLGT", "RLI", "RLJ", "RM", "RMAX", "RMBI", "RMBS", "RMD", "RMNI", "RMR", "RNA", "RNAC", "RNG", "RNGR", "RNR",
        "RNST", "ROAD", "ROCK", "ROG", "ROIV", "ROK", "ROKU", "ROL", "ROOT", "ROP", "ROST", "RPAY", "RPD", "RPM", "RPRX", "RPT", "RR", "RRBI", "RRC", "RRR",
        "RRX", "RS", "RSG", "RSI", "RSVR", "RTX", "RUM", "RUN", "RUSHA", "RUSHB", "RVLV", "RVMD", "RVSB", "RVTY", "RWT", "RXO", "RXRX", "RXST", "RXT", "RYAM",
        "RYAN", "RYI", "RYN", "RYTM", "RZLT", "RZLV",
        
        # S stocks (complete - ends your Russell 2500)
        "S", "SABR", "SAFE", "SAFT", "SAH", "SAIA", "SAIC", "SAIL", "SAM", "SAMG", "SANA", "SANM", "SARO", "SATL", "SATS", "SB", "SBAC", "SBC", "SBCF", "SBFG",
        "SBGI", "SBH", "SBRA", "SBSI", "SBUX", "SCCO", "SCHL", "SCHW", "SCI", "SCL", "SCPH", "SCS", "SCSC", "SCVL", "SD", "SDGR", "SDRL", "SEAT", "SEB", "SEE",
        "SEG", "SEI", "SEIC", "SEM", "SEMR", "SENEA", "SEPN", "SERV", "SEVN", "SEZL", "SF", "SFBC", "SFBS", "SFD", "SFIX", "SFL", "SFM", "SFNC", "SFST", "SG",
        "SGC", "SGHC", "SGHT", "SGI", "SGRY", "SHAK", "SHBI", "SHC", "SHEN", "SHLS", "SHO", "SHOO", "SHW", "SIBN", "SIEB", "SIG", "SIGA", "SIGI", "SILA", "SION",
        "SIRI", "SITC", "SITE", "SITM", "SJM", "SKIL", "SKIN", "SKT", "SKWD", "SKX", "SKY", "SKYH", "SKYT", "SKYW", "SKYX", "SLAB", "SLB", "SLDB", "SLDP", "SLG",
        "SLGN", "SLM", "SLND", "SLNO", "SLP", "SLQT", "SLS", "SLSN", "SLVM", "SM", "SMA", "SMBC", "SMBK", "SMC", "SMCI", "SMG", "SMHI", "SMID", "SMLR", "SMMT",
        "SMP", "SMPL", "SMR", "SMTC", "SMTI", "SN", "SNA", "SNBR", "SNCR", "SNCY", "SNDA", "SNDK", "SNDR", "SNDX", "SNEX", "SNFCA", "SNOW", "SNPS", "SNV", "SNWV",
        "SNX", "SO", "SOC", "SOFI", "SOLV", "SON", "SONO", "SOUN", "SPB", "SPFI", "SPG", "SPGI", "SPHR", "SPIR", "SPNS", "SPNT", "SPOK", "SPOT", "SPR", "SPRY",
        "SPSC", "SPT", "SPTN", "SPWR", "SPXC", "SR", "SRBK",

        # T stocks (150 additions)
        "T", "TANH", "TASK", "TAST", "TATT", "TAYD", "TCBI", "TCBK", "TCDA", "TCFC", "TCMD", "TCON", "TCRT", "TCRR", "TDAC", "TDCX", "TDG", "TDSB", "TEAF", "TEAM",
        "TECH", "TECK", "TECTP", "TDOC", "TELL", "TENB", "TENX", "TER", "TEQX", "TERN", "TERP", "TESS", "TETC", "TFFP", "TFIN", "TFSL", "TGT", "TGAN", "TGLS", "TGNA",
        "TH", "THCH", "THCP", "THFF", "THMO", "THO", "THOR", "THRM", "THRX", "THRY", "THST", "THTX", "TIAA", "TICC", "TIGO", "TILE", "TIPT", "TIRX", "TISI", "TITN",
        "TJX", "TKNO", "TLIS", "TLND", "TLRY", "TLSA", "TLSI", "TMCI", "TMHC", "TMO", "TMPO", "TMST", "TMUS", "TNDM", "TNET", "TNFA", "TNOW", "TNXP", "TOCA", "TOMZ",
        "TORM", "TOUR", "TOWN", "TPAC", "TPCO", "TPIC", "TPST", "TPTX", "TPVG", "TQQQ", "TRAC", "TRCA", "TRCO", "TREB", "TREE", "TREX", "TREV", "TRHC", "TRIB",
        "TRIL", "TRIN", "TRIP", "TRMB", "TRMD", "TRMK", "TRNE", "TRNO", "TRON", "TROW", "TRST", "TRTX", "TRV", "TRUP", "TRVG", "TRVI", "TSCO", "TSEM", "TSHA",
        "TSLX", "TSLA", "TSM", "TSN", "TSQ", "TT", "TTEK", "TTGT", "TTMI", "TTNP", "TTOO", "TTSH", "TTWO", "TUP", "TURN", "TUSK", "TVTX", "TWIN", "TWKS", "TWLO",
        "TWOU", "TWST", "TXMD", "TXN", "TYG", "TYL", "TYME", "TYRA", "TZAC", "TZOO"
        
        # U stocks (75 additions)
        "UAMY", "UAVS", "UBA", "UBCP", "UBER", "UBSI", "UBX", "UCBI", "UCTT", "UDR", "UE", "UEC", "UEIC", "UEPS", "UFCS", "UFI", "UFPI", "UFPT", "UFS", "UGI",
        "UGRO", "UGP", "UHG", "UHS", "UHT", "UI", "UIHC", "UIS", "UK", "UL", "ULBI", "ULH", "ULTA", "ULTI", "UMBF", "UMC", "UNH", "UNITY", "UNP", "UPS",
        "UPST", "UPWK", "URBN", "URI", "UROY", "USA", "USAC", "USAK", "USAP", "USAS", "USB", "USCB", "USCT", "USEA", "USEG", "USER", "USFD", "USLM", "USPH", "UTAA",
        "UTAH", "UTHP", "UTHR", "UTMD", "UTRS", "UTSI", "UTX", "UTZ", "UUUU", "UVE", "UVSP", "UVV", "UWM", "UWMC", "UXIN",
        
        # V stocks (75 additions)
        "V", "VALE", "VAR", "VAXX", "VBFC", "VBIV", "VCEL", "VCIG", "VCRA", "VCTR", "VDOT", "VEDU", "VEEE", "VEEV", "VEL", "VELO", "VERA", "VERB", "VERI", "VERO",
        "VERU", "VERY", "VET", "VFC", "VFF", "VGI", "VGR", "VHAI", "VHC", "VHI", "VIA", "VIAB", "VIAC", "VIAV", "VICI", "VICR", "VID", "VII", "VIKG", "VIR",
        "VIRT", "VIRX", "VISL", "VIST", "VITL", "VIVE", "VIVO", "VJET", "VKI", "VKQ", "VKTX", "VLD", "VLGEA", "VLN", "VLO", "VLRS", "VLT", "VLY", "VMAC", "VMBS",
        "VMC", "VMD", "VMEO", "VMI", "VMOT", "VMW", "VNCE", "VNE", "VNO", "VNOM", "VNRX", "VNT", "VNTV", "VRSK", "VRSN", "VRTX", "VTR", "VZ",
        
        # W stocks (100 additions)
        "WABC", "WAFD", "WAGE", "WAGS", "WAL", "WASH", "WATT", "WAVE", "WBA", "WB", "WBD", "WBS", "WBT", "WBX", "WCC", "WCLD", "WCN", "WD", "WDAY", "WDC", "WDFC",
        "WDH", "WDR", "WDS", "WE", "WEA", "WEBR", "WEC", "WEJO", "WELL", "WEN", "WER", "WES", "WEST", "WEX", "WEYS", "WFRD", "WHR", "WIX", "WKHS", "WLK", "WLL",
        "WM", "WMB", "WMC", "WMG", "WMK", "WMS", "WMT", "WNC", "WNEB", "WNS", "WNW", "WOLF", "WOOF", "WOR", "WORK", "WOW", "WPC", "WPG", "WPM", "WPP", "WPRT",
        "WPS", "WPX", "WQN", "WR", "WRB", "WRBY", "WRE", "WRK", "WRLD", "WRN", "WSBC", "WSBF", "WSC", "WSFS", "WSM", "WSO", "WSR", "WST", "WSTG", "WSTL",
        "WT", "WTBA", "WTI", "WTM", "WTTR", "WTW", "WU", "WUBA", "WVE", "WVFC", "WVT", "WW", "WWD", "WWE", "WWR", "WWW", "WY", "WYND", "WYY",
        
        # X stocks (25 additions)
        "XBIO", "XENE", "XERS", "XLNX", "XLRN", "XMTR", "XNCR", "XNET", "XOG", "XOM", "XOMA", "XOS", "XP", "XPDB", "XPEL", "XPER", "XPEV", "XPL", "XPOF",
        "XPO", "XRAY", "XRX", "XEL", "XYL",
        
        # Y stocks (25 additions)
        "YALA", "YELP", "YETI", "YEXT", "YGMZ", "YI", "YIN", "YJ", "YMAB", "YMM", "YOGA", "YORK", "YORW", "YOU", "YPF", "YQ", "YRD", "YTEN",
        "YTRA", "YUM", "YUMC", "YVR", "YY",
        
        # Z stocks (50 additions)
        "ZAGG", "ZAPP", "ZBRA", "ZCMD", "ZD", "ZEAL", "ZEN", "ZETA", "ZEUS", "ZEV", "ZG", "ZGN", "ZGNX", "ZH", "ZI", "ZIM", "ZIMV", "ZION", "ZIP", "ZIOP",
        "ZIXI", "ZKIN", "ZLAB", "ZM", "ZN", "ZNTL", "ZOM", "ZOON", "ZOOM", "ZS", "ZSAN", "ZSPH", "ZTO", "ZTR", "ZTEST", "ZTRH", "ZUMZ", "ZUO", "ZURA", "ZURAW",
        "ZURVY", "ZVO", "ZVRA", "ZVSA", "ZWRK", "ZWS", "ZYNE", "ZYME", "ZYXI", "ZZCO"
        ]
        
        
        # Remove any duplicates and sort
        all_tickers = sorted(list(set(all_tickers)))
        
        self.logger.info(f"Found {len(all_tickers)} tickers")
        
        # Save ticker list
        ticker_file = self.base_path / "russell3000_tickers.txt"
        with open(ticker_file, 'w') as f:
            f.write('\n'.join(all_tickers))
        
        return all_tickers
    
    def generate_date_chunks(self, start_date: str = "2020-09-08", end_date: str = "2025-09-05") -> List[Tuple[str, str]]:
        """
        Generate optimal date chunks for API calls
        Each chunk should be ~1.5 months to maximize the 50k limit
        """
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        chunks = []
        current = start
        
        while current < end:
            # Add ~45 days (1.5 months) to maximize 50k minute bars
            chunk_end = min(current + timedelta(days=45), end)
            chunks.append((
                current.strftime("%Y-%m-%d"),
                chunk_end.strftime("%Y-%m-%d")
            ))
            current = chunk_end + timedelta(days=1)
        
        return chunks
    
    def download_ticker_data(self, ticker: str, start_date: str = "2020-09-08", end_date: str = "2025-08-29") -> bool:
        """
        Download complete 1-minute OHLCV data for a single ticker
        Saves as {ticker}_1min.csv directly in the main folder
        Returns True if successful, False otherwise
        """
        # Skip if already completed
        if ticker in self.completed_tickers:
            self.logger.info(f"○ {ticker}: Already completed, skipping")
            return True
            
        # Check if file already exists
        output_file = self.base_path / f"{ticker}_1min.csv"  # Save directly to base_path
        if output_file.exists():
            self.logger.info(f"○ {ticker}: File already exists, marking as complete")
            self._save_progress(ticker)
            return True
        
        # Generate date chunks to work within API limits
        date_chunks = self.generate_date_chunks(start_date, end_date)
        all_data = []
        
        self.logger.info(f"⬇ {ticker}: Starting download ({len(date_chunks)} API calls needed)")
        
        for chunk_start, chunk_end in date_chunks:
            url = f"{self.base_url}/v2/aggs/ticker/{ticker}/range/1/minute/{chunk_start}/{chunk_end}"
            params = {
                'adjusted': 'true',
                'sort': 'asc',
                'limit': 50000,
                'apikey': self.api_key
            }
            
            try:
                self.stats['total_api_calls'] += 1
                response = requests.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if 'results' in data and data['results']:
                        # Convert to DataFrame
                        df = pd.DataFrame(data['results'])
                        df['ticker'] = ticker
                        df['timestamp'] = pd.to_datetime(df['t'], unit='ms')
                        
                        # Rename columns to standard OHLCV
                        df = df.rename(columns={
                            'o': 'open',
                            'h': 'high', 
                            'l': 'low',
                            'c': 'close',
                            'v': 'volume',
                            'n': 'transactions'
                        })
                        
                        # Select and order columns (no ticker column in final CSV)
                        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
                        all_data.append(df)
                        
                        self.stats['successful_calls'] += 1
                        self.stats['total_data_points'] += len(df)
                        
                    else:
                        # No data for this period (common for newer stocks or weekends/holidays)
                        self.stats['successful_calls'] += 1
                        
                elif response.status_code == 429:
                    # Rate limited (shouldn't happen with unlimited plan, but just in case)
                    self.logger.warning(f"Rate limited for {ticker}, waiting...")
                    time.sleep(60)
                    continue  # Retry this chunk
                    
                else:
                    self.logger.error(f"✗ {ticker} chunk {chunk_start}-{chunk_end}: HTTP {response.status_code}")
                    self.stats['failed_calls'] += 1
                    
            except Exception as e:
                self.logger.error(f"✗ {ticker} chunk {chunk_start}-{chunk_end}: {str(e)}")
                self.stats['failed_calls'] += 1
                continue
        
        # Combine all data and save
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            final_df = final_df.sort_values('timestamp').drop_duplicates()
            
            # Save as CSV directly to main folder
            final_df.to_csv(output_file, index=False)
            
            self.logger.info(f"✓ {ticker}: Saved {len(final_df):,} records to {ticker}_1min.csv")
            self._save_progress(ticker)
            return True
        else:
            self.logger.warning(f"○ {ticker}: No data found for entire date range")
            # Still mark as complete to avoid retrying
            self._save_progress(ticker)
            return True
    
    def download_all_data(self, max_workers: int = 10):
        """
        Download all Russell 3000 data using parallel processing
        Each ticker gets its own {ticker}_1min.csv file in the main folder
        """
        self.stats['start_time'] = datetime.now()
        self.logger.info("Starting Russell 3000 data download...")
        
        # Get tickers
        tickers = self.get_russell3000_tickers()
        
        # Calculate total work
        total_tickers = len(tickers)
        completed_count = len(self.completed_tickers)
        remaining_tickers = total_tickers - completed_count
        
        self.logger.info(f"Total tickers: {total_tickers}")
        self.logger.info(f"Already completed: {completed_count} tickers")
        self.logger.info(f"Remaining: {remaining_tickers} tickers")
        
        if remaining_tickers == 0:
            self.logger.info("All data already downloaded!")
            return
        
        # Create work queue for remaining tickers
        work_queue = [ticker for ticker in tickers if ticker not in self.completed_tickers]
        
        # Process with thread pool
        self.logger.info(f"Starting download with {max_workers} workers...")
        self.logger.info(f"CSV files will be saved directly to: {self.base_path}")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all jobs
            future_to_ticker = {
                executor.submit(self.download_ticker_data, ticker): ticker
                for ticker in work_queue
            }
            
            # Process completed futures
            for i, future in enumerate(as_completed(future_to_ticker)):
                ticker = future_to_ticker[future]
                
                try:
                    success = future.result()
                    progress_pct = ((i + 1) / len(work_queue)) * 100
                    
                    # Calculate ETA
                    elapsed = datetime.now() - self.stats['start_time']
                    if i > 0:
                        eta_total = elapsed * len(work_queue) / (i + 1)
                        eta_remaining = eta_total - elapsed
                        eta_str = str(eta_remaining).split('.')[0]  # Remove microseconds
                    else:
                        eta_str = "calculating..."
                    
                    self.logger.info(f"Progress: {i+1}/{len(work_queue)} ({progress_pct:.1f}%) - ETA: {eta_str}")
                    
                except Exception as e:
                    self.logger.error(f"Job failed for {ticker}: {e}")
        
        # Final statistics
        elapsed = datetime.now() - self.stats['start_time']
        self.logger.info("=" * 60)
        self.logger.info("DOWNLOAD COMPLETE!")
        self.logger.info(f"Total time: {elapsed}")
        self.logger.info(f"Total API calls: {self.stats['total_api_calls']}")
        self.logger.info(f"Successful calls: {self.stats['successful_calls']}")
        self.logger.info(f"Failed calls: {self.stats['failed_calls']}")
        self.logger.info(f"Total data points: {self.stats['total_data_points']:,}")
        if elapsed.total_seconds() > 0:
            self.logger.info(f"Average calls per second: {self.stats['total_api_calls'] / elapsed.total_seconds():.2f}")
        self.logger.info(f"CSV files saved directly to: {self.base_path}")
        self.logger.info("=" * 60)


def main():
    """
    Main execution function
    """
    # Configuration
    API_KEY = "YOUR_POLYGON_API_KEY_HERE"  # Replace with your actual API key
    DATA_PATH = "DATA_PATH"  # Your existing folder
    MAX_WORKERS = 16  # Adjust based on your system and desired aggressiveness
    
    # Validate API key
    if API_KEY == "YOUR_POLYGON_API_KEY_HERE":
        print("Please set your Polygon.io API key in the script!")
        return
    
    # Create downloader
    downloader = PolygonDataDownloader(
        api_key=API_KEY,
        base_path=DATA_PATH
    )
    
    print(f"CSV files will be saved directly to: {DATA_PATH}")
    print(f"Each stock will get its own file: {{ticker}}_1min.csv")
    print(f"Progress and logs will be saved in subfolders")
    print()
    
    # Start download
    try:
        downloader.download_all_data(max_workers=MAX_WORKERS)
    except KeyboardInterrupt:
        print("\nDownload interrupted by user. Progress has been saved.")
        print("Run the script again to resume from where it left off.")
    except Exception as e:
        print(f"Download failed with error: {e}")
        logging.error(f"Download failed: {e}")


if __name__ == "__main__":
    main()