import random
import asyncio
import nest_asyncio

from loguru import logger
from typing import Optional, Dict, Any
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

nest_asyncio.apply()

MAX_RETRIES = 10
MAX_WAIT_BETWEEN_REQ = 5
MIN_WAIT_BETWEEN_REQ = 2
REQUEST_TIMEOUT = 30000
PAGE_LOAD_TIMEOUT = 60000


class WebScraper:
    def __init__(self):
        self.ua = UserAgent()
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None

    def get_headers(self) -> Dict[str, str]:
        """Generate realistic browser headers"""
        return {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "max-age=0",
            "User-Agent": self.ua.random,
            "Priority": "u=0, i",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Ch-Ua": '"Not(A:Brand";v="99", "Opera GX";v="118", "Chromium";v="133"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1"
        }

    async def setup_browser(self) -> None:
        """Initialize browser and context"""
        if self.browser is None:
            playwright = await async_playwright().start()

            browser_args = {
                "headless": True,
                "args": [
                    "--disable-blink-features=AutomationControlled",
                    "--disable-web-security",
                    "--disable-features=VizDisplayCompositor",
                    "--no-sandbox",
                    "--disable-dev-shm-usage"
                ]
            }

            self.browser = await playwright.chromium.launch(**browser_args)

            context_options = {
                "locale": "en-US",
                "user_agent": self.ua.random,
                "viewport": {"width": 1920, "height": 1080},
                "device_scale_factor": 1,
                "is_mobile": False,
                "has_touch": False,
                "screen": {"width": 1920, "height": 1080},
                "java_script_enabled": True,
                "bypass_csp": True
            }

            self.context = await self.browser.new_context(**context_options)

            # Add stealth script to all pages
            # await self.context.add_init_script("""
            #     Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            #     Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            #     Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
            #     Object.defineProperty(navigator, 'permissions', {get: () => ({query: () => Promise.resolve({state: 'granted'})})});
            #     window.chrome = {runtime: {}};
            #     delete navigator.__proto__.webdriver;
            # """)

    async def simulate_human_behavior(self, page: Page) -> None:
        """Simulate realistic human browsing behavior"""
        try:
            # Random scrolling
            scroll_count = random.randint(3, 6)
            for _ in range(scroll_count):
                scroll_distance = random.randint(300, 700)
                await page.mouse.wheel(0, scroll_distance)
                await asyncio.sleep(random.uniform(0.5, 1.5))

            # Random mouse movements
            move_count = random.randint(5, 10)
            for _ in range(move_count):
                x = random.randint(0, 1920)
                y = random.randint(0, 1080)
                await page.mouse.move(x, y)
                await asyncio.sleep(random.uniform(0.3, 0.8))

            # Occasional random click (not on links)
            if random.random() < 0.3:  # 30% chance
                safe_x = random.randint(100, 500)
                safe_y = random.randint(100, 300)
                await page.mouse.click(safe_x, safe_y)
                await asyncio.sleep(random.uniform(0.5, 1))

        except Exception as e:
            logger.error(f"Error during behavior simulation: {e}")

    @retry(
        wait=wait_exponential(
            multiplier=1, min=MIN_WAIT_BETWEEN_REQ, max=MAX_WAIT_BETWEEN_REQ),
        stop=stop_after_attempt(MAX_RETRIES),
        retry=retry_if_exception_type((asyncio.TimeoutError, Exception)),
        reraise=True,
    )
    async def extract_scrape_content(
        self,
        url: str,
        selector: str,
        timeout: int = REQUEST_TIMEOUT,
        wait_for_network: bool = True,
        simulate_behavior: bool = True
    ) -> Optional[BeautifulSoup]:
        """Extract content with improved retry logic"""

        page = None
        try:
            await self.setup_browser()

            if not self.context:
                raise Exception("Failed to initialize browser context")

            page = await self.context.new_page()

            page.set_default_timeout(timeout)
            page.set_default_navigation_timeout(PAGE_LOAD_TIMEOUT)

            await page.set_extra_http_headers(self.get_headers())

            logger.info(f"Navigating to: {url}")

            wait_until = "networkidle" if wait_for_network else "domcontentloaded"
            response = await page.goto(url, wait_until=wait_until, timeout=PAGE_LOAD_TIMEOUT)

            if not response:
                raise Exception(f"No response received for {url}")

            if response.status >= 400:
                raise Exception(f"HTTP {response.status} error for {url}")

            logger.info(f"Waiting for selector: {selector}")
            await page.wait_for_selector(selector, timeout=timeout)

            if simulate_behavior:
                logger.info("Simulating human behavior...")
                await self.simulate_human_behavior(page)

            logger.info("Extracting page content...")
            rendered_html = await page.content()

            soup = BeautifulSoup(rendered_html, "html.parser")
            logger.info(f"Successfully extracted content from {url}")

            return soup

        except asyncio.TimeoutError as e:
            logger.error(
                f"Timeout waiting for selector '{selector}' on {url}: {e}")
            raise

        except Exception as e:
            logger.error(f"Error scraping {url}: {str(e)}")
            raise

        finally:
            if page:
                try:
                    await page.close()
                except Exception as e:
                    logger.error(f"Error closing page: {e}")

    async def close(self) -> None:
        """Clean up browser resources"""
        try:
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
        except Exception as e:
            logger.error(f"Error closing browser: {e}")
        finally:
            self.browser = None
            self.context = None


class AsyncWebScraper:
    def __init__(self):
        self.scraper = WebScraper()

    async def __aenter__(self):
        return self.scraper

    async def __aexit__(self, *_):
        await self.scraper.close()


async def scrape_url(url, selector) -> list:
    async with AsyncWebScraper() as scraper:
        result = await scraper.extract_scrape_content(url, selector)
        await asyncio.sleep(random.uniform(2, 5))
        return result


async def scrape_urls(urls_and_selectors) -> list:
    async with AsyncWebScraper() as scraper:
        results = []
        for url, selector in urls_and_selectors:
            result = await scraper.extract_scrape_content(url, selector)
            results.append(result)

            await asyncio.sleep(random.uniform(2, 5))
        return results
