'use strict';

(function() {
    const cfg = window.COGNITO_CONFIG || {};
    const { Amplify, Auth } = window.aws_amplify || {};

    if (Amplify && cfg && cfg.userPoolId && cfg.userPoolWebClientId && cfg.region) {
        Amplify.configure({
            Auth: {
                region: cfg.region,
                userPoolId: cfg.userPoolId,
                userPoolWebClientId: cfg.userPoolWebClientId,
                oauth: cfg.domain && cfg.redirectSignIn && cfg.redirectSignOut ? {
                    domain: `${cfg.domain}.auth.${cfg.region}.amazoncognito.com`,
                    scope: ['email', 'openid', 'profile'],
                    redirectSignIn: cfg.redirectSignIn,
                    redirectSignOut: cfg.redirectSignOut,
                    responseType: 'code'
                } : undefined
            }
        });
    }

    const byId = (id) => document.getElementById(id);

    const signupButton = byId('signup-button');
    const confirmButton = byId('confirm-button');
    const signinButton = byId('signin-button');
    const signoutButton = byId('signout-button');

    if (signupButton) {
        signupButton.addEventListener('click', async () => {
            const email = byId('signup-email').value.trim();
            const password = byId('signup-password').value;
            try {
                await Auth.signUp({ username: email, password, attributes: { email } });
                byId('signup-confirm').classList.remove('hidden');
                alert('Sign up initiated. Check your email for the confirmation code.');
            } catch (e) {
                alert('Sign up failed: ' + (e.message || 'Unknown error'));
            }
        });
    }

    if (confirmButton) {
        confirmButton.addEventListener('click', async () => {
            const email = byId('signup-email').value.trim();
            const code = byId('signup-code').value.trim();
            try {
                await Auth.confirmSignUp(email, code);
                alert('Account confirmed. You can now sign in.');
            } catch (e) {
                alert('Confirmation failed: ' + (e.message || 'Unknown error'));
            }
        });
    }

    if (signinButton) {
        signinButton.addEventListener('click', async () => {
            const email = byId('signin-email').value.trim();
            const password = byId('signin-password').value;
            try {
                await Auth.signIn(email, password);
                alert('Signed in successfully.');
            } catch (e) {
                alert('Sign in failed: ' + (e.message || 'Unknown error'));
            }
        });
    }

    if (signoutButton) {
        signoutButton.addEventListener('click', async () => {
            try {
                await Auth.signOut();
                alert('Signed out.');
            } catch (e) {
                alert('Sign out failed: ' + (e.message || 'Unknown error'));
            }
        });
    }
})();


