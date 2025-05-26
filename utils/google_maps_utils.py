from crawlee.crawlers import PlaywrightCrawlingContext


async def google_map_consent_check(context: PlaywrightCrawlingContext):
	if context.page.url.startswith('https://consent.google.com/m?continue='):
		try:
			# Wait for the 'Reject All' button to be visible
			reject_all_button = await context.page.query_selector("button[aria-label*='Reject All']")
			
			# Click the button and wait for navigation
			await reject_all_button.click()
			context.log.info("Navigated to the next page after rejecting consent.")
		except Exception as e:
			context.log.info(f"Error handling consent: {e}")
