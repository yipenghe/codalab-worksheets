import React from 'react';
import {
    BrowserRouter as Router,
    Route,
    Link,
    Redirect,
    withRouter,
    Switch,
} from 'react-router-dom';
import { CookiesProvider, withCookies } from 'react-cookie';
import UserInfo from './components/UserInfo';
import PublicHome from './components/PublicHome';
import $ from 'jquery';

////////////////////////////////////////////////////////////
// 1. Click the public page
// 2. Click the protected page
// 3. Log in
// 4. Click the back button, note the URL each time

function CodalabApp() {
    return (
        <CookiesProvider>
            <Router>
                <div>
                    <AuthButton />
                    <ul>
                        <li>
                            <Link to='/'>Public Page</Link>
                        </li>
                        <li>
                            <Link to='/account/profile'>User Info Page</Link>
                        </li>
                    </ul>
                    <Switch>
                        <Route path='/' exact component={PublicHome} />
                        <Route path='/login' component={Login} />
                        <PrivateRoute path='/account/profile' component={UserInfo} />
                        <Route component={NoPage} />
                    </Switch>
                </div>
            </Router>
        </CookiesProvider>
    );
}

const fakeAuth = {
    isAuthenticated: false,
    authenticate(authObject, callback) {
        $.ajax({
            type: 'POST',
            url: '/rest/account/login',
            data: {
                username: authObject.username,
                password: authObject.password,
            },
            //or your custom data either as object {foo: "bar", ...} or foo=bar&...
            success: function(response, status, xhr) {
                fakeAuth.isAuthenticated = true;
                console.log(response);
                console.log(status);
                console.log(xhr);
                if (callback) {
                    callback();
                }
            },
        });
    },
    signout(cb) {
        this.isAuthenticated = false;
        setTimeout(cb, 100);
    },
};

const AuthButton = withRouter(({ history, cookies }) =>
    fakeAuth.isAuthenticated ? (
        <p>
            Welcome!{' '}
            <button
                onClick={() => {
                    fakeAuth.signout(() => history.push('/'));
                }}
            >
                Sign out
            </button>
        </p>
    ) : (
        <p>You are not logged in.</p>
    ),
);

const PrivateRoute = ({ component: Component, ...rest }) => (
    <Route
        {...rest}
        component={(props) =>
            fakeAuth.isAuthenticated ? (
                <Component {...props} />
            ) : (
                <Redirect
                    to={{
                        pathname: '/login',
                        state: { from: props.location },
                    }}
                />
            )
        }
    />
);

function NoPage() {
    return <div>404 No Page Exists</div>;
}

function Public() {
    return <h3>Public</h3>;
}

function Protected() {
    return <h3>Protected</h3>;
}

class Login extends React.Component {
    state = { redirectToReferrer: false, username: '', password: '' };

    login = (e) => {
        e.preventDefault();
        fakeAuth.authenticate(
            { username: this.state.username, password: this.state.password },
            () =>
                this.setState(() => ({
                    redirectToReferrer: true,
                })),
        );
    };

    handleInputChange = (event) => {
        const target = event.target;
        const value = target.value;
        const name = target.name;

        this.setState({
            [name]: value,
        });
    };

    render() {
        let { from } = this.props.location.state || { from: { pathname: '/' } };
        let { redirectToReferrer } = this.state;

        if (redirectToReferrer) return <Redirect to={from} />;

        return (
            <div>
                <p>You must log in to view the page at {from.pathname}</p>
                <form className='login' method='POST' onSubmit={this.login}>
                    <div className='form-group'>
                        <label htmlFor='id_login'>Login:</label>
                        <input
                            id='id_login'
                            className='form-control'
                            name='username'
                            placeholder='Username or e-mail'
                            type='text'
                            autoFocus=''
                            autoComplete='off'
                            value={this.state.username}
                            onChange={this.handleInputChange}
                        />
                    </div>
                    <div className='form-group'>
                        <label htmlFor='id_password'>Password:</label>
                        <input
                            id='id_password'
                            className='form-control'
                            name='password'
                            placeholder='Password'
                            type='password'
                            autoComplete='off'
                            value={this.state.password}
                            onChange={this.handleInputChange}
                        />
                    </div>
                    {/* the above is almost certainly wrong, not sure how to fix*/}
                    <button type='submit'>Sign In</button>
                </form>
            </div>
        );
    }
}

export default CodalabApp;
