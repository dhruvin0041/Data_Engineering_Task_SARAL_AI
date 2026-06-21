async def login_with_credentials(
    page: Page,
    email: Optional[str] = None,
    password: Optional[str] = None,
    timeout: int = 30000,
    warm_up: bool = True
) -> None:
    """
    Login to LinkedIn using email and password.
    
    Args:
        page: Playwright page object
        email: LinkedIn email (if None, tries to load from .env)
        password: LinkedIn password (if None, tries to load from .env)
        timeout: Timeout in milliseconds
        warm_up: Whether to warm up browser by visiting normal sites first
        
    Raises:
        AuthenticationError: If login fails
    """
    # Load from .env if not provided
    if not email or not password:
        env_email, env_password = load_credentials_from_env()
        email = email or env_email
        password = password or env_password
    
    if not email or not password:
        raise AuthenticationError(
            "LinkedIn credentials not provided. "
            "Either pass email/password parameters or set LINKEDIN_EMAIL "
            "and LINKEDIN_PASSWORD in your .env file."
        )
    
    # Warm up browser first to appear more human-like
    if warm_up:
        await warm_up_browser(page)
    
    logger.info("Logging in to LinkedIn...")
    
    try:
        # Navigate to login page
        await page.goto('https://www.linkedin.com/login', wait_until='domcontentloaded')
        
        # Check for rate limiting
        await detect_rate_limit(page)
        
        # Wait for login form
        try:
            await page.wait_for_selector('#username', timeout=timeout, state='visible')
        except PlaywrightTimeoutError:
            raise AuthenticationError(
                "Login form not found. LinkedIn may have changed their page structure "
                "or the site is experiencing issues."
            )
        
        # Fill in credentials
        await page.fill('#username', email)
        await page.fill('#password', password)
        
        logger.debug("Credentials entered")
        
        # Click sign in button
        await page.click('button[type="submit"]')
        
        # Wait for navigation
        try:
            await page.wait_for_url(
                lambda url: 'feed' in url or 'checkpoint' in url or 'authwall' in url,
                timeout=timeout
            )
        except PlaywrightTimeoutError:
            # Check if we're still on login page
            if 'login' in page.url:
                raise AuthenticationError(
                    "Login failed. Please check your credentials. "
                    "The page did not navigate after clicking sign in."
                )
        
        # Check for various post-login states
        current_url = page.url
        
        # Check for security checkpoint
        if 'checkpoint' in current_url or 'challenge' in current_url:
            raise AuthenticationError(
                "LinkedIn security checkpoint detected. "
                "You may need to verify your identity manually. "
                "Consider using session persistence after manual verification. "
                f"Current URL: {current_url}"
            )
        
        # Check for auth wall
        if 'authwall' in current_url:
            raise AuthenticationError(
                "Authentication wall encountered. "
                "LinkedIn may be blocking automated access. "
                f"Current URL: {current_url}"
            )
        
        # Verify we're logged in by polling is_logged_in()
        start_time = time.time()
        logged_in = False
        while (time.time() - start_time) * 1000 < 5000:
            if await is_logged_in(page):
                logger.info("✓ Successfully logged in to LinkedIn")
                logged_in = True
                break
            await asyncio.sleep(0.5)  # Poll every 500ms
        
        if not logged_in:
            # Timeout: couldn't verify within 5s but may still be logged in
            logger.warning(
                "Could not verify login by finding navigation element. "
                "Proceeding anyway..."
            )
    
    except PlaywrightTimeoutError as e:
        raise AuthenticationError(
            f"Login timed out: {e}. "
            "This could indicate network issues or LinkedIn blocking the request."
        )
    except Exception as e:
        if isinstance(e, AuthenticationError):
            raise
        raise AuthenticationError(f"Unexpected error during login: {e}")
