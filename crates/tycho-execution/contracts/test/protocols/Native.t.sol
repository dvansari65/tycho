// SPDX-License-Identifier: BUSL-1.1
pragma solidity ^0.8.26;

import "../TychoRouterTestSetup.sol";
import "@src/executors/NativeExecutor.sol";
import {TransferManager} from "@src/TransferManager.sol";
import {Constants} from "../Constants.sol";
import "forge-std/Test.sol";

// Mocks

contract MockNativeRouter {
    bool public called;
    uint256 public lastValue;
    bytes public lastCalldata;
    address public lastCaller;

    receive() external payable {}

    fallback() external payable {
        called = true;
        lastValue = msg.value;
        lastCalldata = msg.data;
        lastCaller = msg.sender;
    }

    function reset() external {
        called = false;
        lastValue = 0;
        lastCalldata = hex"";
        lastCaller = address(0);
    }
}

contract MockNativeTarget {
    IERC20 constant USDC = IERC20(0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48);
    uint256 constant AMOUNT_IN = 3000000000;

    receive() external payable {}

    fallback() external payable {
        USDC.transferFrom(msg.sender, address(this), AMOUNT_IN);
        payable(msg.sender).transfer(1000);
    }
}

contract MockNativeEthTarget {
    IERC20 constant USDC = IERC20(0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48);
    uint256 constant AMOUNT_IN = 1 ether;
    uint256 constant AMOUNT_OUT = 1_000_000;

    fallback() external payable {
        require(msg.value == AMOUNT_IN, "Incorrect msg.value");
        USDC.transfer(msg.sender, AMOUNT_OUT);
    }
}

// Unit Tests

contract NativeExecutorUnitTest is Test, Constants {
    NativeExecutor executor;
    MockNativeRouter mockV4;
    MockNativeRouter mockV3;
    MockNativeRouter mockVault;

    address constant BAD_TARGET = address(0xdead);
    bytes4 constant ALLOWED_SELECTOR = 0x0947c2d9;

    function setUp() public {
        mockV4 = new MockNativeRouter();
        mockV3 = new MockNativeRouter();
        mockVault = new MockNativeRouter();

        executor = new NativeExecutor(
            address(mockV4), address(mockV3), address(mockVault)
        );
    }

    // Constructor tests

    function test_Constructor_StoresAddresses() public view {
        assertEq(executor.nativeRouterV4(), address(mockV4));
        assertEq(executor.nativeRouterV3(), address(mockV3));
        assertEq(executor.creditVault(), address(mockVault));
    }

    function test_Constructor_Reverts_ZeroAddress() public {
        vm.expectRevert(NativeExecutor__ZeroAddress.selector);
        new NativeExecutor(address(0), address(mockV3), address(mockVault));

        vm.expectRevert(NativeExecutor__ZeroAddress.selector);
        new NativeExecutor(address(mockV4), address(mockV3), address(0));
    }

    function test_Constructor_AllowsUnsupportedV3() public {
        NativeExecutor executorWithoutV3 =
            new NativeExecutor(address(mockV4), address(0), address(mockVault));

        assertEq(executorWithoutV3.nativeRouterV4(), address(mockV4));
        assertEq(executorWithoutV3.nativeRouterV3(), address(0));
        assertEq(executorWithoutV3.creditVault(), address(mockVault));

        bytes memory data = _encodeExecutorData(
            USDC_ADDR,
            ETH_ADDR,
            address(mockV4),
            0,
            abi.encodePacked(ALLOWED_SELECTOR)
        );
        executorWithoutV3.getTransferData(data);
    }

    function test_Constructor_Reverts_NotAContract() public {
        address eoa = makeAddr("eoa");

        vm.expectRevert(NativeExecutor__NotAContract.selector);
        new NativeExecutor(eoa, address(mockV3), address(mockVault));

        vm.expectRevert(NativeExecutor__NotAContract.selector);
        new NativeExecutor(address(mockV4), eoa, address(mockVault));

        vm.expectRevert(NativeExecutor__NotAContract.selector);
        new NativeExecutor(address(mockV4), address(mockV3), eoa);
    }

    // fundsExpectedAddress tests

    function test_FundsExpectedAddress_ReturnsMsgSender() public view {
        assertEq(executor.fundsExpectedAddress(hex""), address(this));
    }

    // getTransferData tests

    function test_GetTransferData_ERC20() public view {
        bytes memory payload = abi.encodePacked(ALLOWED_SELECTOR, hex"abcd");
        bytes memory data = _encodeExecutorData(
            USDC_ADDR, ETH_ADDR, address(mockV4), 0, payload
        );

        (
            TransferManager.TransferType transferType,
            address receiver,
            address tokenIn,
            address tokenOut,
            bool outputToRouter
        ) = executor.getTransferData(data);

        assertEq(
            uint8(transferType),
            uint8(TransferManager.TransferType.ProtocolWillDebit)
        );
        assertEq(receiver, address(mockV4));
        assertEq(tokenIn, USDC_ADDR);
        assertEq(tokenOut, ETH_ADDR);
        assertTrue(outputToRouter);
    }

    function test_GetTransferData_NativeETH() public view {
        bytes memory payload = abi.encodePacked(ALLOWED_SELECTOR, hex"abcd");
        bytes memory data = _encodeExecutorData(
            ETH_ADDR, USDC_ADDR, address(mockV3), 1 ether, payload
        );

        (
            TransferManager.TransferType transferType,
            address receiver,
            address tokenIn,
            address tokenOut,
            bool outputToRouter
        ) = executor.getTransferData(data);

        assertEq(
            uint8(transferType),
            uint8(TransferManager.TransferType.TransferNativeInExecutor)
        );
        assertEq(receiver, address(0));
        assertEq(tokenIn, ETH_ADDR);
        assertEq(tokenOut, USDC_ADDR);
        assertTrue(outputToRouter);
    }

    function test_GetTransferData_Reverts_InvalidTarget() public {
        bytes memory payload = abi.encodePacked(ALLOWED_SELECTOR);
        bytes memory data =
            _encodeExecutorData(USDC_ADDR, ETH_ADDR, BAD_TARGET, 0, payload);

        vm.expectRevert(NativeExecutor__InvalidTarget.selector);
        executor.getTransferData(data);
    }

    function test_GetTransferData_Reverts_ZeroTarget_WhenV3Unsupported()
        public
    {
        NativeExecutor executorWithoutV3 = new NativeExecutor(
            address(mockV4), address(0), address(mockVault)
        );
        bytes memory data = _encodeExecutorData(
            USDC_ADDR,
            ETH_ADDR,
            address(0),
            0,
            abi.encodePacked(ALLOWED_SELECTOR)
        );

        vm.expectRevert(NativeExecutor__InvalidTarget.selector);
        executorWithoutV3.getTransferData(data);
    }

    function test_GetTransferData_Reverts_ShortData() public {
        vm.expectRevert(NativeExecutor__InvalidDataLength.selector);
        executor.getTransferData(hex"1234");
    }

    // swap tests

    function test_Swap_ERC20_SendsZeroEth() public {
        bytes memory payload =
            abi.encodeWithSelector(ALLOWED_SELECTOR, hex"1234");
        bytes memory data = _encodeExecutorData(
            USDC_ADDR, ETH_ADDR, address(mockV4), 0, payload
        );

        executor.swap(1000000, data, address(0));

        assertTrue(mockV4.called());
        assertEq(mockV4.lastValue(), 0);
        assertEq(mockV4.lastCaller(), address(executor));
    }

    function test_Swap_AllowsV3Target() public {
        bytes memory payload = abi.encodeWithSelector(ALLOWED_SELECTOR);
        bytes memory data = _encodeExecutorData(
            USDC_ADDR, ETH_ADDR, address(mockV3), 0, payload
        );

        executor.swap(1_000_000, data, address(0));

        assertTrue(mockV3.called());
        assertEq(mockV3.lastValue(), 0);
    }

    function test_Swap_AllowsCreditVaultTarget() public {
        bytes memory payload = abi.encodeWithSelector(ALLOWED_SELECTOR);
        bytes memory data = _encodeExecutorData(
            USDC_ADDR, ETH_ADDR, address(mockVault), 0, payload
        );

        executor.swap(1_000_000, data, address(0));

        assertTrue(mockVault.called());
        assertEq(mockVault.lastValue(), 0);
    }

    function test_Swap_ETH_CappedByAmountIn() public {
        bytes memory payload =
            abi.encodeWithSelector(ALLOWED_SELECTOR, hex"1234");
        bytes memory data = _encodeExecutorData(
            ETH_ADDR, USDC_ADDR, address(mockV4), 2 ether, payload
        );

        vm.deal(address(this), 1 ether);
        executor.swap{value: 1 ether}(1 ether, data, address(0));

        assertTrue(mockV4.called());
        assertEq(mockV4.lastValue(), 1 ether);
    }

    function test_Swap_ETH_CappedByApiValue() public {
        bytes memory payload =
            abi.encodeWithSelector(ALLOWED_SELECTOR, hex"1234");
        bytes memory data = _encodeExecutorData(
            ETH_ADDR, USDC_ADDR, address(mockV4), 0.5 ether, payload
        );

        vm.deal(address(this), 1 ether);
        executor.swap{value: 1 ether}(1 ether, data, address(0));

        assertTrue(mockV4.called());
        assertEq(mockV4.lastValue(), 0.5 ether);
    }

    function test_Swap_ETH_ZeroAmountInAndZeroValue() public {
        bytes memory payload =
            abi.encodeWithSelector(ALLOWED_SELECTOR, hex"1234");
        bytes memory data = _encodeExecutorData(
            ETH_ADDR, USDC_ADDR, address(mockV4), 0, payload
        );

        executor.swap(0, data, address(0));

        assertTrue(mockV4.called());
        assertEq(mockV4.lastValue(), 0);
    }

    function test_Swap_NonETH_IgnoresApiValue() public {
        bytes memory payload =
            abi.encodeWithSelector(ALLOWED_SELECTOR, hex"1234");
        bytes memory data = _encodeExecutorData(
            USDC_ADDR, ETH_ADDR, address(mockV4), 1 ether, payload
        );

        executor.swap(1000000, data, address(0));

        assertTrue(mockV4.called());
        assertEq(mockV4.lastValue(), 0);
    }

    function test_Swap_Reverts_InvalidTarget() public {
        bytes memory payload =
            abi.encodeWithSelector(ALLOWED_SELECTOR, hex"1234");
        bytes memory data =
            _encodeExecutorData(USDC_ADDR, ETH_ADDR, BAD_TARGET, 0, payload);

        vm.expectRevert(NativeExecutor__InvalidTarget.selector);
        executor.swap(1000000, data, address(0));
    }

    function test_Swap_Reverts_ZeroTarget_WhenV3Unsupported() public {
        NativeExecutor executorWithoutV3 =
            new NativeExecutor(address(mockV4), address(0), address(mockVault));
        bytes memory data = _encodeExecutorData(
            USDC_ADDR,
            ETH_ADDR,
            address(0),
            0,
            abi.encodePacked(ALLOWED_SELECTOR)
        );

        vm.expectRevert(NativeExecutor__InvalidTarget.selector);
        executorWithoutV3.swap(1000000, data, address(0));
    }

    function test_Swap_Reverts_InvalidSelector() public {
        bytes4 badSelector = 0x12345678;
        bytes memory payload = abi.encodeWithSelector(badSelector, hex"1234");
        bytes memory data = _encodeExecutorData(
            USDC_ADDR, ETH_ADDR, address(mockV4), 0, payload
        );

        vm.expectRevert(NativeExecutor__InvalidPayload.selector);
        executor.swap(1000000, data, address(0));
    }

    function test_Swap_Reverts_EmptyPayload() public {
        bytes memory data =
            _encodeExecutorData(USDC_ADDR, ETH_ADDR, address(mockV4), 0, hex"");

        vm.expectRevert(NativeExecutor__InvalidPayload.selector);
        executor.swap(1000000, data, address(0));
    }

    function test_Swap_Reverts_ShortData() public {
        vm.expectRevert(NativeExecutor__InvalidDataLength.selector);
        executor.swap(1000000, hex"1234", address(0));
    }

    // Helper

    function _encodeExecutorData(
        address tokenIn,
        address tokenOut,
        address target,
        uint256 value,
        bytes memory payload
    ) internal pure returns (bytes memory) {
        return abi.encodePacked(
            bytes20(tokenIn),
            bytes20(tokenOut),
            bytes20(target),
            bytes32(value),
            payload
        );
    }
}

// Integration Tests

contract TychoRouterNativeIntegrationTest is TychoRouterTestSetup {
    function getForkBlock() public pure override returns (uint256) {
        return 23624157;
    }

    function test_SingleSwap() public {
        IERC20 USDC = IERC20(USDC_ADDR);
        uint256 amountIn = 3000000000;

        deal(address(USDC), ALICE, amountIn);
        uint256 balanceBefore = BOB.balance;

        vm.startPrank(ALICE);
        USDC.approve(tychoRouterAddr, type(uint256).max);

        address target = NATIVE_ROUTER_V4;
        MockNativeTarget mock = new MockNativeTarget();
        vm.etch(target, address(mock).code);
        deal(target, 10 ether);

        bytes memory callData =
            loadCallDataFromFile("test_single_encoding_strategy_native");

        (bool success,) = tychoRouterAddr.call(callData);

        uint256 balanceAfter = BOB.balance;
        assertTrue(success, "Call Failed");
        assertEq(balanceAfter - balanceBefore, 1000);
        assertEq(USDC.balanceOf(tychoRouterAddr), 0);
    }

    function test_SingleSwapNativeInput() public {
        IERC20 USDC = IERC20(USDC_ADDR);
        uint256 amountIn = 1 ether;
        uint256 amountOut = 1_000_000;

        address target = NATIVE_ROUTER_V4;
        MockNativeEthTarget mock = new MockNativeEthTarget();
        vm.etch(target, address(mock).code);
        deal(address(USDC), target, amountOut);
        deal(ALICE, amountIn);

        bytes memory callData = loadCallDataFromFile(
            "test_single_encoding_strategy_native_eth_input"
        );
        uint256 balanceBefore = USDC.balanceOf(BOB);

        vm.prank(ALICE);
        (bool success,) = tychoRouterAddr.call{value: amountIn}(callData);

        assertTrue(success, "Call Failed");
        assertEq(USDC.balanceOf(BOB) - balanceBefore, amountOut);
        assertEq(target.balance, amountIn);
        assertEq(USDC.balanceOf(tychoRouterAddr), 0);
        assertEq(tychoRouterAddr.balance, 0);
    }
}
