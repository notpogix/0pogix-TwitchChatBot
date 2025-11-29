const crypto = require('crypto');
const axios = require('axios');

const clientId = process.env.SPOTIFY_CLIENT_ID;
const clientSecret = process.env.SPOTIFY_CLIENT_SECRET;
const redirectUri = process.env.SPOTIFY_REDIRECT_URI;

const authUrl = 'https://accounts.spotify.com/authorize';
const tokenUrl = 'https://accounts.spotify.com/api/token';
const apiUrl = 'https://api.spotify.com/v1';

function generateCodeVerifier() {
  return crypto.randomBytes(32).toString('hex');
}

function generateCodeChallenge(verifier) {
  return crypto.createHash('sha256').update(verifier).digest('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=/g, '');
}

function getAuthorizationUrl(username) {
  const codeVerifier = generateCodeVerifier();
  const codeChallenge = generateCodeChallenge(codeVerifier);
  
  const scope = 'user-read-currently-playing';
  const params = new URLSearchParams({
    client_id: clientId,
    response_type: 'code',
    redirect_uri: redirectUri,
    scope: scope,
    code_challenge_method: 'S256',
    code_challenge: codeChallenge,
    state: username
  });

  return {
    url: `${authUrl}?${params.toString()}`,
    codeVerifier: codeVerifier
  };
}

async function exchangeCodeForToken(code, codeVerifier) {
  try {
    const response = await axios.post(tokenUrl, new URLSearchParams({
      client_id: clientId,
      client_secret: clientSecret,
      code: code,
      redirect_uri: redirectUri,
      grant_type: 'authorization_code',
      code_verifier: codeVerifier
    }).toString(), {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
    });

    return {
      accessToken: response.data.access_token,
      refreshToken: response.data.refresh_token,
      expiresAt: Date.now() + (response.data.expires_in * 1000)
    };
  } catch (err) {
    console.error('Error exchanging code for token:', err.message);
    throw err;
  }
}

async function refreshAccessToken(refreshToken) {
  try {
    const response = await axios.post(tokenUrl, new URLSearchParams({
      client_id: clientId,
      client_secret: clientSecret,
      refresh_token: refreshToken,
      grant_type: 'refresh_token'
    }).toString(), {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
    });

    return {
      accessToken: response.data.access_token,
      refreshToken: response.data.refresh_token || refreshToken,
      expiresAt: Date.now() + (response.data.expires_in * 1000)
    };
  } catch (err) {
    console.error('Error refreshing token:', err.message);
    throw err;
  }
}

async function getCurrentlyPlaying(accessToken) {
  try {
    const response = await axios.get(`${apiUrl}/me/player/currently-playing`, {
      headers: { 'Authorization': `Bearer ${accessToken}` }
    });

    if (response.status === 204 || !response.data.item) {
      return null;
    }

    const item = response.data.item;
    const trackName = item.name;
    const artists = item.artists.map(a => a.name).join(', ');
    const albumName = item.album.name;

    return {
      name: trackName,
      artists: artists,
      album: albumName,
      url: item.external_urls.spotify,
      isPlaying: response.data.is_playing
    };
  } catch (err) {
    console.error('Error getting currently playing:', err.message);
    return null;
  }
}

module.exports = {
  getAuthorizationUrl,
  exchangeCodeForToken,
  refreshAccessToken,
  getCurrentlyPlaying,
  generateCodeVerifier,
  generateCodeChallenge
};
