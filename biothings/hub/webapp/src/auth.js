import bus from './bus.js'
import hubapi from './hubapi.js'
import cognitoIDs from './cognitoIDs.js'

import { CognitoUserPool,
         CognitoUserAttribute,
         CognitoUser,
         CognitoUserSession,
         CookieStorage,
         AuthenticationDetails,
} from 'amazon-cognito-identity-js';


const userPool = new CognitoUserPool(cognitoIDs);

class AuthService {

  signIn(username, password) {
    // cognito boiler place code
    const authData = {
        UserName: username,
        Password: password,
        Storage: new CookieStorage({"domain":window.location.hostname})
    }
    const authDetails = new AuthenticationDetails(authData);
    const userData = {
        Username: username,
        Pool: userPool,
        Storage: new CookieStorage({"domain":window.location.hostname}),
    }
    const cognitoUser = new CognitoUser(userData);
    cognitoUser.authenticateUser(authDetails, {
      onSuccess (result) {
        bus.$emit("logged",username,result);
      },
      onFailure(err) {
        bus.$emit("logerror",username,err.name,err.message);
      }
    });
  }

  signOut() {
    userPool.client.request('GlobalSignOut', {
      AccessToken: hubapi.getAccessToken(),
    }, err => {
      if(err) {
        bus.$emit("logerror",err.name,err.message);
      }
    });

  }

  refreshAccessToken() {
    // using refresh token, ask for a new access token
    // if refresh token has expired, user is logged out
    // Note: aws sdk does that automagically for us, but tokens
    // needs to be stored in app storage, not safe, so we have to
    // do some of the stuff manually.
    const authParameters = {"REFRESH_TOKEN" : hubapi.getRefreshToken()};
    const jreq = {
      ClientId: userPool.getClientId(),
			AuthFlow: 'REFRESH_TOKEN_AUTH',
			AuthParameters: authParameters,
			//ClientMetadata: clientMetadata
    }

    userPool.client.request('InitiateAuth', jreq,(err, authResult) => {
      if(err) {
        console.log("Error while refreshing access token:");
        console.log(err);
        // refresh token might have expired ?
        this.signOut();
        hubapi.clearLoggedUser();
      } else {
        var newtoken = authResult.AuthenticationResult.AccessToken;
        document.cookie = "biothings-access-token=" + newtoken;
      }
    });
  }
}

export default new AuthService();
